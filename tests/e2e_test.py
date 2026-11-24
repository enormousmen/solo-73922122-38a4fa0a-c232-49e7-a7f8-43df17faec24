"""端到端关键操作测试（迁移 + 业务账号轮换）。

两种运行方式：
 1. 对运行中的 Flask 服务发真实 HTTP 请求：
       BASE_URL=http://localhost:8000 python tests/e2e_test.py
 2. 进程内模式（默认，使用 Flask test client，走完整 WSGI 栈）：
       PGPORT=5433 python tests/e2e_test.py

覆盖场景：
 A. 迁移（原有 8 个场景，业务连接改走业务账号后仍须全部通过）：
    初始无列与样例数据、等锁、等锁期间普通读写毫秒级完成且无排队 DDL、
    撤销后释锁也不执行、释锁自动续跑两列同生效、批次号可写可读、
    故障注入两表结构照旧、重置恢复。
 B. 账号轮换：
    1. 初始状态：app_a 启用可登录、app_b 为 NOLOGIN，状态不泄露口令；
    2. 成功轮换：备用账号生成新口令、验证两表读写（验证数据不落库）、
       活动账号翻转、旧账号 NOLOGIN、新账号业务读写正常；
    3. 等待验证期间新发起的查单/试写仍由原账号完成；
    4. 同一时间只受理一次轮换（在途再发起返回 409）；
    5. 故障注入：验证失败提示原因、沿用原账号、备用账号恢复 NOLOGIN、
       失败后可重试并成功；
    6. 切换前打开的演示事务在切换后仍可手动结束；旧账号新建会话被
       PostgreSQL 拒绝；
    7. 重启对账：成功切换后"重启"状态保留；轮换在途时"重启"沿用原
       账号并可重试。
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

# 确保可以 import 到工作区里的 app 包
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_URL = os.environ.get("BASE_URL")  # 不设置则走进程内 test client
SLOW_MS = float(os.environ.get("SLOW_MS", "500"))  # 等锁/验证期普通读写阈值
# 进程内测试用独立的状态目录，避免与手工演示互相干扰
TEST_STATE_DIR = os.environ.get("ROTATION_TEST_STATE_DIR", "/tmp/rotation-test-state")

PASS, FAIL = 0, 0

if BASE_URL:
    def call(method: str, path: str, body: dict | None = None
             ) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            BASE_URL + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())
else:
    os.environ["ROTATION_STATE_DIR"] = TEST_STATE_DIR
    from app import app, init_app
    from app import db as db_module
    from app.rotation import reset_manager_for_restart

    init_app()
    _client = app.test_client()

    def call(method: str, path: str, body: dict | None = None
             ) -> tuple[int, dict]:
        # 注意：test client 的 open() 返回字符串状态文本（"200 OK"），
        # get()/post() 才返回整数状态码，这里统一用后者
        kwargs = {"headers": {"Content-Type": "application/json"}}
        if body is not None:
            kwargs["data"] = json.dumps(body)
        if method == "GET":
            resp = _client.get(path, **kwargs)
        else:
            resp = _client.post(path, **kwargs)
        return resp.status_code, resp.get_json()


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")

def timed(method: str, path: str, body=None) -> tuple[float, int, dict]:
    t0 = time.perf_counter()
    status, data = call(method, path, body)
    ms = (time.perf_counter() - t0) * 1000
    return ms, status, data


def wait_rotation(states: set[str], timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = call("GET", "/api/state")
        if last["rotation"]["state"] in states:
            return last
        time.sleep(0.1)
    raise AssertionError(f"轮换状态未进入 {states}，最后: {last.get('rotation')}")


def wait_state(states: set[str], timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = call("GET", "/api/state")
        if last["migration"]["state"] in states:
            return last
        time.sleep(0.2)
    raise AssertionError(f"状态未进入 {states}，最后状态: {last}")


def reset() -> None:
    # 异常中断可能残留演示事务，先全部清理，保证重复运行可收敛
    _, state = call("GET", "/api/state")
    for tx in list(state.get("held_transactions", [])):
        call("POST", "/api/demo-tx/close", {"token": tx["token"]})
    call("POST", "/api/reset")
    wait_state({"idle", "succeeded"})


def no_queued_ddl(state: dict) -> bool:
    """服务器端锁队列里不应有未授予（排队中）的锁。"""
    return all(row["granted"] for row in state["server_lock_queue"])


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def role_flags(state: dict) -> dict[str, bool]:
    return {r["role"]: r["can_login"] for r in state["rotation"]["roles"]}


# ---------------------------------------------------------------- 迁移场景
def test_migration() -> None:
    section("A1. 初始状态与样例数据")
    reset()
    _, state = call("GET", "/api/state")
    check("两表初始均无 batch_no",
          state["columns"]["orders"] is False
          and state["columns"]["order_items"] is False)
    _, orders = call("GET", "/api/orders")
    check("样例订单为 2 笔", len(orders) == 2, f"实际 {len(orders)}")
    check("样例明细共 3 行",
          sum(len(o["items"]) for o in orders) == 3)

    section("A2. 演示事务 + 迁移进入等待")
    _, tx = call("POST", "/api/demo-tx/open", {"table": "orders"})
    check("演示事务已打开且使用业务账号",
          tx.get("token", "").startswith("tx-") and tx.get("role") in
          ("app_a", "app_b"))
    call("POST", "/api/migration/submit", {})
    state = wait_state({"waiting"})
    check("迁移状态=等待锁中", state["migration"]["state"] == "waiting")

    section("A3. 等锁期间普通读写不受阻")
    ms, status, _ = timed("GET", "/api/orders")
    check(f"普通读成功且耗时 < {SLOW_MS}ms（实测 {ms:.1f}ms）",
          status == 200 and ms < SLOW_MS)
    payload = {"customer": "压测客户",
               "items": [{"sku": "SKU-T", "quantity": 1, "unit_price": "1.00"}]}
    ms, status, data = timed("POST", "/api/orders", payload)
    check(f"普通写成功且耗时 < {SLOW_MS}ms（实测 {ms:.1f}ms）",
          status == 201 and ms < SLOW_MS, f"resp={data}")
    ms, status, _ = timed("GET", "/api/orders")
    check(f"再次普通读 {ms:.1f}ms 成功", (status == 200 and ms < SLOW_MS))
    _, state = call("GET", "/api/state")
    check("服务器端无排队中（granted=false）的 DDL 锁",
          no_queued_ddl(state), str(state["server_lock_queue"]))
    check("迁移仍处于等待、尝试次数已增长",
          state["migration"]["state"] == "waiting"
          and state["migration"]["attempts"] >= 1)

    section("A4. 撤销后即使释锁也不执行")
    _, cancelled = call("POST", "/api/migration/cancel")
    check("撤销接口返回已撤销", cancelled["state"] == "cancelled")
    call("POST", "/api/demo-tx/close", {"token": tx["token"]})
    time.sleep(2)  # 超过两个轮询周期
    _, state = call("GET", "/api/state")
    check("撤销后释锁，两列仍未生效",
          not any(state["columns"].values()), str(state["columns"]))
    check("状态保持已撤销", state["migration"]["state"] == "cancelled")

    section("A5. 释锁后迁移自动继续，两列一同生效")
    _, tx2 = call("POST", "/api/demo-tx/open", {"table": "order_items"})
    call("POST", "/api/migration/submit", {})
    wait_state({"waiting"})
    call("POST", "/api/demo-tx/close", {"token": tx2["token"]})
    state = wait_state({"succeeded"}, timeout=15)
    check("迁移完成", state["migration"]["state"] == "succeeded")
    check("orders.batch_no 已生效", state["columns"]["orders"] is True)
    check("order_items.batch_no 已生效",
          state["columns"]["order_items"] is True)

    section("A6. 生效后批次号可写可读")
    body = {"customer": "批次客户", "batch_no": "B-E2E-001",
            "items": [{"sku": "SKU-B1", "quantity": 2, "unit_price": "3.33"}]}
    status, data = call("POST", "/api/orders", body)
    check("带批次号写入成功", status == 201, str(data))
    _, orders = call("GET", "/api/orders")
    target = next(o for o in orders if o["id"] == data["order_id"])
    check("订单批次号读回正确", target["batch_no"] == "B-E2E-001")
    check("明细批次号同步写入",
          all(i["batch_no"] == "B-E2E-001" for i in target["items"]))

    section("A7. 故障注入：失败时两表结构照旧")
    reset()
    _, state = call("GET", "/api/state")
    check("重置后两列均不存在", not any(state["columns"].values()))
    _, tx3 = call("POST", "/api/demo-tx/open", {"table": "orders"})
    call("POST", "/api/migration/submit", {"inject_failure": True})
    wait_state({"waiting"})
    call("POST", "/api/demo-tx/close", {"token": tx3["token"]})
    state = wait_state({"failed"}, timeout=15)
    check("迁移状态=失败（已回滚）", state["migration"]["state"] == "failed")
    check("失败原因已记录", bool(state["migration"]["error"]))
    _, cols = call("GET", "/api/columns")
    check("orders 结构照旧（无 batch_no）",
          cols["orders"]["batch_no"] is False)
    check("order_items 结构照旧（无 batch_no）",
          cols["order_items"]["batch_no"] is False)
    _, orders = call("GET", "/api/orders")
    check("失败后普通读仍可用、样例数据完好", len(orders) == 2)

    section("A8. 清理：重置恢复初始演示环境")
    reset()
    _, state = call("GET", "/api/state")
    check("重置后迁移状态=空闲", state["migration"]["state"] == "idle")
    check("重置后无打开的演示事务",
          state["held_transactions"] == [])


# ---------------------------------------------------------------- 轮换场景
def test_rotation() -> None:
    section("B1. 初始账号状态与口令不泄露")
    _, state = call("GET", "/api/state")
    rot = state["rotation"]
    check("初始当前账号为 app_a", rot["active_role"] == "app_a")
    check("轮换状态为空闲", rot["state"] == "idle")
    flags = role_flags(state)
    check("app_a 允许 LOGIN", flags["app_a"] is True)
    check("app_b 为 NOLOGIN", flags["app_b"] is False)
    raw = json.dumps(rot)
    check("接口返回不含口令字段",
          "password" not in raw and "credentials" not in raw, raw[:200])

    section("B2. 成功轮换：验证不落库、活动账号翻转、旧账号 NOLOGIN")
    _, before_orders = call("GET", "/api/orders")
    before_ids = {o["id"] for o in before_orders}
    status, data = call("POST", "/api/rotation/start", {})
    check("轮换受理成功", status == 200, str(data))
    state = wait_rotation({"succeeded"})
    rot = state["rotation"]
    check("轮换成功", rot["state"] == "succeeded")
    check("当前账号翻转为 app_b", rot["active_role"] == "app_b")
    check("成功轮换次数=1", rot["generation"] == 1)
    flags = role_flags(state)
    check("app_b 允许 LOGIN", flags["app_b"] is True)
    check("app_a 已置 NOLOGIN", flags["app_a"] is False)
    _, after_orders = call("GET", "/api/orders")
    after_ids = {o["id"] for o in after_orders}
    check("验证订单未落库（订单集合不变）", after_ids == before_ids,
          f"before={before_ids} after={after_ids}")
    check("页面无 ROTATION-CHECK 残留",
          all("ROTATION-CHECK" not in o["order_no"] for o in after_orders))
    # 新账号业务读写
    payload = {"customer": "轮换后客户",
               "items": [{"sku": "SKU-R1", "quantity": 1, "unit_price": "2.50"}]}
    status, data = call("POST", "/api/orders", payload)
    check("新账号 app_b 试写成功", status == 201, str(data))
    status, _ = call("GET", "/api/orders")
    check("新账号 app_b 查单成功", status == 200)

    section("B3. 等待验证期间业务仍走原账号")
    # 先把账号切回 app_a，保证本小节起点明确
    call("POST", "/api/rotation/start", {})
    wait_rotation({"succeeded"})
    _, state = call("GET", "/api/state")
    check("起点当前账号=app_a", state["rotation"]["active_role"] == "app_a")
    # 注入 3s 验证窗口（docker-compose 默认带，进程内测试显式设置）
    os.environ["ROTATION_VERIFY_DELAY"] = "3"
    try:
        call("POST", "/api/rotation/start", {})
        wait_rotation({"verifying"})
        time.sleep(0.5)  # 确保仍在验证窗口内
        _, state = call("GET", "/api/state")
        check("轮换处于验证中", state["rotation"]["state"] == "verifying")
        check("验证中活动指针仍是原账号 app_a",
              state["rotation"]["active_role"] == "app_a")
        ms, status, data = timed("GET", "/api/orders")
        check(f"验证期间查单成功且 < {SLOW_MS}ms（{ms:.1f}ms）",
              status == 200 and ms < SLOW_MS)
        payload = {"customer": "验证期客户",
                   "items": [{"sku": "SKU-W", "quantity": 1,
                              "unit_price": "1.00"}]}
        ms, status, data = timed("POST", "/api/orders", payload)
        check(f"验证期间试写成功且 < {SLOW_MS}ms（{ms:.1f}ms）",
              status == 201 and ms < SLOW_MS, str(data))
        state = wait_rotation({"succeeded"})
        check("验证窗口结束后翻转为 app_b",
              state["rotation"]["active_role"] == "app_b")
    finally:
        os.environ.pop("ROTATION_VERIFY_DELAY", None)

    section("B4. 同一时间只受理一次轮换")
    os.environ["ROTATION_VERIFY_DELAY"] = "3"
    try:
        call("POST", "/api/rotation/start", {})
        wait_rotation({"verifying"})
        status, data = call("POST", "/api/rotation/start", {})
        check("在途再次发起被拒绝（409）", status == 409, str(data))
        check("提示同一时间只受理一次", "只受理一次" in data.get("error", ""))
        wait_rotation({"succeeded"})
    finally:
        os.environ.pop("ROTATION_VERIFY_DELAY", None)

    section("B5. 故障注入：失败提示原因、沿用原账号、可重试成功")
    _, state = call("GET", "/api/state")
    active_before = state["rotation"]["active_role"]
    standby_before = state["rotation"]["standby_role"]
    status, _ = call("POST", "/api/rotation/start", {"inject_failure": True})
    check("故障轮换受理", status == 200)
    state = wait_rotation({"failed"})
    rot = state["rotation"]
    check("轮换状态=失败", rot["state"] == "failed")
    check("失败原因已提示",
          bool(rot["error"]) and "password" in rot["error"].lower())
    check("沿用原账号", rot["active_role"] == active_before)
    flags = role_flags(state)
    check("原账号仍可登录", flags[active_before] is True)
    check("备用账号已恢复 NOLOGIN", flags[standby_before] is False)
    # 失败后业务仍可用
    status, _ = call("GET", "/api/orders")
    check("失败后查单仍可用", status == 200)
    # 重新发起（不带故障注入）应成功
    call("POST", "/api/rotation/start", {})
    state = wait_rotation({"succeeded"})
    check("重试后轮换成功", state["rotation"]["state"] == "succeeded")
    check("重试后账号翻转",
          state["rotation"]["active_role"] == standby_before)

    section("B6. 旧账号新建会话被拒；切换前演示事务仍可手动结束")
    # 先回到 app_a 作为活动账号，再打开演示事务
    _, state = call("GET", "/api/state")
    if state["rotation"]["active_role"] != "app_a":
        call("POST", "/api/rotation/start", {})
        wait_rotation({"succeeded"})
    _, tx = call("POST", "/api/demo-tx/open", {"table": "orders"})
    check("演示事务以 app_a 打开", tx["role"] == "app_a")
    # 轮换到 app_b：app_a 被 NOLOGIN
    call("POST", "/api/rotation/start", {})
    state = wait_rotation({"succeeded"})
    check("当前账号=app_b", state["rotation"]["active_role"] == "app_b")
    check("app_a 已 NOLOGIN", role_flags(state)["app_a"] is False)
    if not BASE_URL:
        # 进程内模式：直接用旧账号口令尝试新建会话，PostgreSQL 必须拒绝。
        # HTTP 模式下测试进程不持有服务端口令，改由上面的 can_login
        # 标志（查询 pg_roles.rolcanlogin）佐证 NOLOGIN 已生效。
        import psycopg2
        from app import db as _db
        from app.rotation import get_manager
        # 取服务端保存的 app_a 口令（仅进程内可见，用于验证服务端拒绝）
        pwd_a = get_manager()._credentials["app_a"]
        params = _db.dsn()
        try:
            psycopg2.connect(host=params["host"], port=params["port"],
                             dbname=params["dbname"], user="app_a",
                             password=pwd_a, connect_timeout=5)
            check("旧账号 app_a 新建会话被 PostgreSQL 拒绝", False, "居然连接成功")
        except psycopg2.Error as exc:
            # NOLOGIN 触发 28000（invalid_authorization_specification），
            # 个别 psycopg2 版本不回填 SQLSTATE，因此同时按错误文本兜底
            msg = str(exc)
            check("旧账号 app_a 新建会话被 PostgreSQL 拒绝",
                  exc.pgcode in ("28000", "28P01")
                  or "not permitted to log in" in msg, msg)
    else:
        check("HTTP 模式：pg_roles 显示 app_a 已无登录权（NOLOGIN）",
              role_flags(state)["app_a"] is False)
    # 切换前打开的演示事务（既有会话）仍可手动结束
    _, held = call("GET", "/api/state")
    check("切换后演示事务仍在",
          any(t["token"] == tx["token"] for t in held["held_transactions"]))
    status, data = call("POST", "/api/demo-tx/close", {"token": tx["token"]})
    check("切换前打开的演示事务可手动结束", status == 200, str(data))


def test_restart() -> None:
    """重启对账：进程内重新走 bootstrap，等价于服务在持久化卷上重启。"""
    if BASE_URL:
        print("\n=== B7. 重启对账（仅进程内模式执行，HTTP 模式请手动重启容器）===")
        return
    section("B7a. 成功切换后重启：切换保留")
    _, state = call("GET", "/api/state")
    if state["rotation"]["active_role"] != "app_a":
        call("POST", "/api/rotation/start", {})
        wait_rotation({"succeeded"})
    call("POST", "/api/rotation/start", {})
    state = wait_rotation({"succeeded"})
    check("重启前当前账号=app_b", state["rotation"]["active_role"] == "app_b")
    mgr = reset_manager_for_restart(TEST_STATE_DIR)
    _, state = call("GET", "/api/state")
    check("重启后当前账号仍为 app_b（切换保留）",
          state["rotation"]["active_role"] == "app_b")
    check("重启后轮换状态为空闲", state["rotation"]["state"] == "idle")
    flags = role_flags(state)
    check("重启后 app_b 可登录、app_a 为 NOLOGIN",
          flags["app_b"] is True and flags["app_a"] is False)
    status, _ = call("GET", "/api/orders")
    check("重启后业务读正常", status == 200)
    status, data = call("POST", "/api/orders", {
        "customer": "重启后客户",
        "items": [{"sku": "SKU-RS", "quantity": 1, "unit_price": "1.00"}]})
    check("重启后业务写正常", status == 201, str(data))
    check("内存管理器与单例一致", mgr.active_role_name() == "app_b")

    section("B7b. 轮换在途时重启：沿用原账号并可重试")
    # 先回到 app_a 作为活动账号
    _, state = call("GET", "/api/state")
    if state["rotation"]["active_role"] != "app_a":
        call("POST", "/api/rotation/start", {})
        wait_rotation({"succeeded"})
    # 精确模拟"进程在轮换验证阶段被杀死"：直接把 in_flight 状态文件
    # 写盘（真实崩溃时活动指针尚未翻转），再重新走启动对账
    from app.rotation import get_manager
    mgr = get_manager()
    crash_payload = {
        "version": 1,
        "active_role": "app_a",
        "credentials": dict(mgr._credentials),
        "generation": mgr.generation,
        "in_flight": True,
        "last_result": None,
        "updated_at": "crash-simulation",
    }
    tmp = mgr.state_file + ".crash"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(crash_payload, fh)
    os.replace(tmp, mgr.state_file)
    mgr2 = reset_manager_for_restart(TEST_STATE_DIR)
    _, state = call("GET", "/api/state")
    rot = state["rotation"]
    check("在途重启后沿用原账号 app_a", rot["active_role"] == "app_a")
    check("在途重启后状态=空闲、可重新发起", rot["state"] == "idle")
    check("在途重启记录了中断提示", rot["last_result"] == "interrupted")
    flags = role_flags(state)
    check("对账后 app_a 可登录、app_b 为 NOLOGIN",
          flags["app_a"] is True and flags["app_b"] is False)
    status, _ = call("GET", "/api/orders")
    check("重启后原账号查单可用", status == 200)
    # 重试轮换应成功
    call("POST", "/api/rotation/start", {})
    state = wait_rotation({"succeeded"})
    check("中断后重试轮换成功，翻转为 app_b",
          state["rotation"]["active_role"] == "app_b")


def main() -> int:
    print(f"测试目标: {BASE_URL or '进程内 test client'}  "
          f"时间: {datetime.now():%Y-%m-%d %H:%M:%S}")
    test_migration()
    test_rotation()
    test_restart()
    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
