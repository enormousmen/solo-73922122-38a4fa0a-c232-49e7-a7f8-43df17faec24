# 订单库账号在线轮换 + 批次号在线迁移演示（Flask + PostgreSQL）

两个运维场景的在线化演示：

1. **业务账号轮换**：订单读写与演示读事务使用两个轮流启用的
   PostgreSQL 业务账号 `app_a` / `app_b`，每次给备用账号生成新口令，
   实际连库验证两张业务表都能读能写（验证数据不落库）后才切换；
2. **批次号迁移**：给 `orders`（订单表）和 `order_items`（明细表）
   **一起**加上可空的批次号列 `batch_no VARCHAR(64)`，等锁期间普通
   读写不受影响，失败整体回滚、两表结构照旧。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- `db`：postgres:16-alpine，带健康检查；
- `web`：Flask 应用，容器入口先等数据库就绪、建库建表并写入样例
  订单（2 笔订单、3 行明细），再创建/对账两个业务账号并启动服务；
- 命名卷 `rotation-state` 挂载到 `/var/lib/order-demo`，保存当前业务
  账号与**仅服务端使用**的口令，支撑服务重启后的状态保留。

停止并清空数据：

```bash
docker compose down -v   # 同时删除数据库卷与轮换状态卷
```

## 页面操作路径

1. **① 演示读事务**：选择 `orders` 或 `order_items`，用当前业务账号
   打开一个不提交、不回滚的读事务（持有表级 `AccessShareLock`），
   需要时手动结束；
2. **② 业务账号轮换**：查看当前/备用账号与登录权，发起轮换；可勾选
   故障注入观察失败回退；
3. **③ 加列迁移申请**：提交后状态进入"等待锁中"，可故障注入、可撤销；
4. **④ 试写订单/明细**：轮换验证期间与迁移等锁期间都可写入和查询，
   页面显示耗时；迁移完成后可同时写入订单与明细的批次号；
5. **⑤ 列生效情况**、**⑥ pg_locks 实况**、**⑦ 样例数据**与
   **⑧ 事件日志**（迁移 + 轮换分开标注）：页面每秒自动刷新。

## 核心设计一：业务账号轮换（app/rotation.py）

### 账号模型

- **管理员账号**（`postgres` 超级用户，来自 `PGUSER/PGPASSWORD`）：
  只用于建库、DDL、`CREATE/ALTER ROLE`、`GRANT` 等管理动作；
- **业务账号**（`app_a` / `app_b`）：订单读写、演示读事务等业务
  连接一律使用当前启用的业务账号。任一时刻只有一个账号 `LOGIN`，
  另一个被置 `NOLOGIN`，PostgreSQL 直接拒绝其新建会话。

### 轮换流程（后台线程，single-flight）

1. 给**备用账号生成新口令**（`secrets` 生成 28 位随机串），并临时
   放开它的 `LOGIN` 以便验证；
2. 用备用账号**实际连库**，在一个事务里对 `orders`、`order_items`
   分别做 `SELECT` 与 `INSERT`，确认两表都能读能写，最后
   **`ROLLBACK`——验证订单不落库**；
3. 验证期间活动账号指针不翻转，因此新发起的查单/试写仍由**原账号**
   完成（compose 默认 `ROTATION_VERIFY_DELAY=3s` 拉长验证窗口便于观察）；
4. 验证通过后，先把新活动账号（含新口令）**原子写入状态文件并
   fsync**，再把旧账号置 `NOLOGIN`：
   - 业务连接此后只用新账号建连；
   - 旧账号新建会话被 PostgreSQL 拒绝（`FATAL: role ... is not
     permitted to log in`）；
   - 切换前已打开的演示事务属于**既有会话**，`NOLOGIN` 不影响它，
     值班员仍可手动结束；
5. 验证失败时记录原因并把备用账号恢复 `NOLOGIN`，活动指针从未翻转，
   业务继续用原账号，可重新发起（再次生成新口令重试）；
6. 同一时间只受理一次轮换：在途再次发起返回 HTTP 409。

### 崩溃安全与重启对账

状态文件 `rotation-state.json`（权限 600）记录活动账号、口令、
轮换代数与 `in_flight` 标志，写入采用临时文件 + `os.replace` 原子
替换。切换顺序保证任意一步崩溃都能收敛：

| 崩溃时刻 | 重启对账行为 |
| --- | --- |
| 验证阶段（指针未翻转） | 判定 `interrupted`，沿用原账号，备用账号补 `NOLOGIN`，可重试 |
| 状态文件已翻转、旧账号尚未 NOLOGIN | 以状态文件为准，新账号生效，补把旧账号置 `NOLOGIN` |
| 切换完成后 | 当前账号与口令原样保留，业务无缝继续 |

口令只保存在服务端内存与状态文件中，`/api/state` 等所有接口与
页面**均不返回口令**。

## 核心设计二：为什么等锁期间普通读写不被堵住（app/migration.py）

PostgreSQL 中 `ALTER TABLE ... ADD COLUMN` 需要 `ACCESS EXCLUSIVE`
锁。若直接发起而表上有未结束事务，ALTER 会进入**服务器端锁等待
队列**，后续到达该表的普通读写会排在 ALTER 后面一起被阻塞。

本项目的迁移线程因此**不进入服务器端锁队列**：

1. 每 0.5s 用独立连接查 `pg_locks`，判断两表上是否存在与
   `ACCESS EXCLUSIVE` 冲突的已授予表级锁（共 7 种模式）；
2. 无冲突时才在**同一个数据库事务**里执行两条 `ALTER TABLE`，
   并设置 `lock_timeout=250ms` 兜底；
3. 兜底超时（SQLSTATE `55P03`）则回滚本轮、重新进入等待循环；
4. 两张表的 ALTER 同事务提交：任一失败整体回滚，保证两表结构一致；
5. 撤销通过 `threading.Event` 置位，等待循环与执行前双重检查，
   确认撤销后永不执行。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/api/state` | 迁移/轮换状态、两列是否生效、演示事务、pg_locks 实况 |
| POST | `/api/demo-tx/open` | 用当前业务账号在任一表打开保持不结束的读事务 |
| POST | `/api/demo-tx/close` | 手动结束演示事务（释放锁） |
| POST | `/api/migration/submit` | 提交加列申请（`inject_failure` 可故障注入） |
| POST | `/api/migration/cancel` | 撤销等待中的申请 |
| POST | `/api/rotation/start` | 发起一次业务账号轮换（`inject_failure` 可故障注入） |
| GET/POST | `/api/orders` | 查询订单明细 / 试写订单+明细（走当前业务账号） |
| GET  | `/api/columns` | 两表 batch_no 列是否存在 |
| POST | `/api/reset` | 重建两表与样例数据（迁移/轮换进行中或有演示事务时拒绝） |

## 测试

```bash
# 进程内模式（Flask test client，走完整 WSGI 栈）
PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres \
PGDATABASE=orders_demo python tests/e2e_test.py

# 或对已运行的服务发真实 HTTP
BASE_URL=http://localhost:8000 python tests/e2e_test.py
```

覆盖 16 个场景、78 项断言（进程内模式）：

- **迁移 A1–A8**：初始无列与样例数据、等锁、等锁期间普通读写毫秒级
  完成且服务器端无排队 DDL、撤销后释锁也不执行、释锁自动续跑两列
  同生效、批次号可写可读、故障注入两表结构照旧、重置恢复；
- **轮换 B1–B6**：初始账号与口令不泄露、成功轮换（验证不落库、指针
  翻转、旧账号 NOLOGIN、新账号读写正常）、验证窗口内业务仍走原账号、
  single-flight 409、故障注入失败提示原因并沿用原账号且可重试、
  旧账号新建会话被拒而切换前演示事务仍可手动结束；
- **重启 B7a/B7b**：成功切换后重启状态保留；在途重启沿用原账号并可
  重试（B7 仅进程内模式执行，HTTP 模式可通过重启容器手工验证）。

## 目录结构

```
app/
  __init__.py        Flask 路由与演示事务管理
  __main__.py        python -m app 入口
  db.py              管理员/业务连接、建表、角色与授权、订单读写
  migration.py       迁移状态机与应用层等锁/重试/撤销
  rotation.py        业务账号轮换状态机、验证、切换、重启对账
  templates/index.html  单页界面
tests/e2e_test.py    端到端测试
Dockerfile / docker-compose.yml / docker-entrypoint.sh
```

## 已知边界

- 迁移状态保存在进程内存，重启后通过检查列是否存在恢复为
  "已完成/空闲"；轮换状态则持久化在状态文件中，重启可对账恢复。
- 探测间隔 0.5s、兜底 `lock_timeout` 250ms，是为演示把影响压到
  亚秒级；高并发生产环境建议再评估参数。
- `ROTATION_VERIFY_DELAY` 仅为演示拉长验证窗口，生产环境留空即可。
- 状态文件与数据库卷是两个独立持久化单元：`docker compose down -v`
  会同时清空；若只清库不清状态卷，重启对账会以状态文件为准修复
  角色与口令，业务仍可继续。
- `ADD COLUMN ... 可空无默认值` 在 PG 11+ 本身是快速元数据操作，
  本演示重点是锁等待行为与原子性，不涉及大表重写。
