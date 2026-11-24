"""业务账号在线轮换管理器。

需求要点
========
1. 订单读写与演示读事务使用两个轮流启用的 PostgreSQL 业务账号
   （``app_a`` / ``app_b``），任一时刻只有一个账号允许新建会话；
2. 每次轮换给**备用账号生成新口令**，先用备用账号实际连库，验证
   ``orders`` / ``order_items`` 两张业务表都能读能写（验证数据随事务
   回滚，**不落库**），全部通过后才切换；
3. 验证等待期间，新发起的查单/试写仍由**原账号**完成——活动账号指针
   只有在验证成功并持久化后才翻转；
4. 切换成功后业务只用新账号建连，并把旧账号置为 ``NOLOGIN``，
   PostgreSQL 会拒绝旧账号的新建会话；切换前已打开的演示事务属于
   既有会话，不受 NOLOGIN 影响，仍可手动结束；
5. 验证失败时记录原因、恢复备用账号的 NOLOGIN 状态，业务继续用
   原账号，值班员可重新发起轮换（再次生成新口令重试）；
6. 同一时间只受理一次轮换（single-flight）；
7. 网页服务在轮换中重启：已持久化的切换必须保留，未完成则沿用原
   账号并可重试。当前账号与口令保存在服务端状态文件（Docker 命名卷
   持久化），重启时做对账（reconcile）；
8. 口令只在服务端使用：状态快照与页面/接口均不返回口令。

崩溃安全的切换顺序
==================
验证通过后严格按以下顺序执行，保证任意一步崩溃后重启对账都能收敛：
  a. 先把新活动账号（含新口令）原子写入状态文件并 fsync；
  b. 再把旧账号置为 NOLOGIN。
若在 a 之后、b 之前崩溃：重启后以状态文件为准，新账号是活动账号，
对账会把旧账号补成 NOLOGIN，切换依然生效。
"""
from __future__ import annotations

import json
import os
import secrets
import string
import threading
import time
from collections import deque
from datetime import datetime, timezone

import psycopg2

from . import db

# 状态机
IDLE = "idle"                # 无轮换在途
VERIFYING = "verifying"      # 备用账号口令已轮换、正在实际连库验证
SWITCHING = "switching"      # 验证通过，正在持久化并禁用旧账号
SUCCEEDED = "succeeded"      # 轮换成功
FAILED = "failed"            # 验证/切换失败，沿用原账号，可重试

TERMINAL_STATES = {SUCCEEDED, FAILED}
IN_FLIGHT_STATES = {VERIFYING, SWITCHING}

STATE_VERSION = 1
DEFAULT_STATE_DIR = "/var/lib/order-demo"
STATE_FILENAME = "rotation-state.json"

_PWD_ALPHABET = string.ascii_letters + string.digits + "-_"
_PWD_LENGTH = 28


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")


def generate_password() -> str:
    """生成新口令。每次轮换备用账号都重新生成。"""
    return "".join(secrets.choice(_PWD_ALPHABET) for _ in range(_PWD_LENGTH))


class RotationManager:
    def __init__(self, state_dir: str | None = None) -> None:
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self.state_dir = state_dir or os.environ.get(
            "ROTATION_STATE_DIR", DEFAULT_STATE_DIR
        )
        self.state_file = os.path.join(self.state_dir, STATE_FILENAME)

        self.state = IDLE
        self.error: str | None = None
        self.active_role: str = db.ROLE_A
        self.standby_role: str = db.ROLE_B
        self.last_started_at: str | None = None
        self.last_finished_at: str | None = None
        self.last_result: str | None = None       # succeeded / failed / interrupted
        self.generation = 0                       # 成功轮换次数
        # 口令仅保存在内存与服务端状态文件中，绝不通过接口/页面输出
        self._credentials: dict[str, str] = {}
        self.events: deque[dict] = deque(maxlen=200)
        self._log("轮换管理器就绪")

    # ------------------------------------------------------------ 日志
    def _log(self, message: str) -> None:
        self.events.appendleft({"time": _now(), "message": message})

    def get_events(self) -> list[dict]:
        with self._lock:
            return list(self.events)

    # ------------------------------------------------------------ 状态文件
    def _read_state_file(self) -> dict | None:
        try:
            with open(self.state_file, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except FileNotFoundError:
            return None

    def _persist_locked(self, status: str | None = None) -> None:
        """把当前活动账号与口令原子落盘（同目录临时文件 + rename + fsync）。"""
        os.makedirs(self.state_dir, exist_ok=True)
        payload = {
            "version": STATE_VERSION,
            "active_role": self.active_role,
            "credentials": dict(self._credentials),
            "generation": self.generation,
            "in_flight": self.state in IN_FLIGHT_STATES,
            "last_result": status or self.last_result,
            "updated_at": _now(),
        }
        tmp = f"{self.state_file}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.state_file)
        try:
            os.chmod(self.state_file, 0o600)
        except OSError:  # pragma: no cover - 个别文件系统不支持
            pass

    # ------------------------------------------------------------ 启动对账
    def bootstrap(self) -> None:
        """进程启动时调用：建/修业务角色并与持久化状态对账。

        - 首次启动（无状态文件）：生成两个账号的初始口令，app_a 启用、
          app_b 置 NOLOGIN，落盘后返回；
        - 重启：读回活动账号与口令，修复可能缺失/漂移的 ROLE；若上次
          轮换在途时进程退出，则判定为 interrupted，活动账号指针不翻转，
          业务沿用原账号，值班员可重试。
        """
        with self._lock:
            data = self._read_state_file()
            if data is None:
                self._log("首次启动：创建两个业务账号并生成初始口令")
                pwd_a = generate_password()
                pwd_b = generate_password()
                db.ensure_business_role(db.ROLE_A, pwd_a)
                db.ensure_business_role(db.ROLE_B, pwd_b)
                self._credentials = {db.ROLE_A: pwd_a, db.ROLE_B: pwd_b}
                self.active_role = db.ROLE_A
                self.standby_role = db.ROLE_B
                # 备用账号默认拒绝新建会话
                db.set_role_login(db.ROLE_B, False)
                self.generation = 0
                self.last_result = None
                self.state = IDLE
                self._persist_locked()
                self._log(
                    "业务账号就绪：app_a 当前启用，app_b 已置 NOLOGIN"
                )
                return

            saved_active = data.get("active_role", db.ROLE_A)
            saved_creds = data.get("credentials") or {}
            self.generation = int(data.get("generation", 0))
            if saved_active not in db.BUSINESS_ROLES:
                saved_active = db.ROLE_A
            self.active_role = saved_active
            self.standby_role = (
                db.ROLE_B if saved_active == db.ROLE_A else db.ROLE_A
            )

            # 修复角色缺失/口令漂移：以状态文件里的口令为准
            for role in db.BUSINESS_ROLES:
                pwd = saved_creds.get(role) or generate_password()
                saved_creds[role] = pwd
                db.ensure_business_role(role, pwd)
            self._credentials = dict(saved_creds)

            interrupted = bool(data.get("in_flight"))
            if interrupted:
                # 切换未完成即重启：活动指针未翻转即沿用原账号
                self.state = IDLE
                self.last_result = "interrupted"
                self.error = (
                    "检测到上次轮换在完成前重启，已沿用原账号，可重新发起轮换"
                )
                self._log("上次轮换在途时进程退出，判定为中断，沿用原账号")
            else:
                self.state = IDLE
                self.last_result = data.get("last_result")
                self.error = None
                self._log(
                    f"重启恢复：当前业务账号 {self.active_role}"
                    + ("，上次轮换已生效并保留"
                       if self.last_result == "succeeded" else "")
                )

            # 以状态文件为准对账 LOGIN 标志：活动账号可登录，其余 NOLOGIN
            for role in db.BUSINESS_ROLES:
                db.set_role_login(role, role == self.active_role)
            self._persist_locked()

    # ------------------------------------------------------------ 对外状态
    def active_credentials(self) -> tuple[str, str]:
        with self._lock:
            return self.active_role, self._credentials[self.active_role]

    def active_role_name(self) -> str:
        with self._lock:
            return self.active_role

    def snapshot(self) -> dict:
        """页面/接口状态。注意：绝不包含口令。"""
        with self._lock:
            login_flags = {
                role: db.role_can_login(role) for role in db.BUSINESS_ROLES
            }
            return {
                "state": self.state,
                "state_label": _STATE_LABELS.get(self.state, self.state),
                "active_role": self.active_role,
                "standby_role": self.standby_role,
                "roles": [
                    {
                        "role": role,
                        "active": role == self.active_role,
                        "can_login": login_flags[role],
                    }
                    for role in db.BUSINESS_ROLES
                ],
                "error": self.error,
                "generation": self.generation,
                "last_started_at": self.last_started_at,
                "last_finished_at": self.last_finished_at,
                "last_result": self.last_result,
                "in_flight": self.state in IN_FLIGHT_STATES,
                "events": list(self.events),
            }

    # ------------------------------------------------------------ 发起轮换
    def submit(self, inject_failure: bool = False) -> None:
        """发起一次轮换；已有轮换在途时拒绝（single-flight）。"""
        with self._lock:
            if self.state in IN_FLIGHT_STATES:
                raise RuntimeError("已有轮换正在进行，同一时间只受理一次")
            self.state = VERIFYING
            self.error = None
            self.last_started_at = _now()
            self.last_finished_at = None
            self.last_result = None
            self._log(
                f"收到轮换申请：备用账号 {self.standby_role} 将生成新口令，"
                "先实际连库验证两表读写再切换"
                + ("（已勾选故障注入）" if inject_failure else "")
            )
            self._thread = threading.Thread(
                target=self._run,
                args=(inject_failure,),
                name="account-rotation",
                daemon=True,
            )
            self._thread.start()

    # ------------------------------------------------------------ 后台流程
    def _run(self, inject_failure: bool) -> None:
        standby = self.standby_role
        active = self.active_role
        try:
            # 步骤 1：给备用账号生成全新口令，并临时放开 LOGIN 以便验证
            new_password = generate_password()
            with self._lock:
                self._log(f"已为备用账号 {standby} 生成新口令（仅服务端保存）")
            db.set_role_password(standby, new_password, login=True)

            # 测试钩子：模拟验证耗时，让"等待验证期间业务仍走原账号"可观测
            delay = float(os.environ.get("ROTATION_VERIFY_DELAY", "0") or 0)
            if delay > 0:
                time.sleep(delay)

            # 步骤 2：用备用账号实际连库验证两张业务表的读写
            with self._lock:
                self._log(f"正在用 {standby} 实际连库验证两表读写（数据不落库）")
            self._verify_standby(standby, new_password, inject_failure)

            # 步骤 3：验证通过——先持久化翻转活动指针（崩溃安全），
            # 此时新发起的业务连接已改用新账号
            with self._lock:
                self.state = SWITCHING
                self._credentials[standby] = new_password
                self.active_role = standby
                self.standby_role = active
                self.generation += 1
                self.last_result = "succeeded"
                self._persist_locked()
                self._log(
                    f"验证通过，活动账号已切换为 {standby} 并持久化；"
                    f"新发起的业务连接只使用 {standby}"
                )

            # 步骤 4：旧账号置 NOLOGIN，PostgreSQL 拒绝其新建会话；
            # 既有会话（如切换前打开的演示事务）不受影响
            db.set_role_login(active, False)
            with self._lock:
                self.state = SUCCEEDED
                self.last_finished_at = _now()
                self._persist_locked()
                self._log(
                    f"旧账号 {active} 已置 NOLOGIN，新建会话将被 PostgreSQL 拒绝；"
                    "切换前已打开的演示事务仍可手动结束"
                )
        except Exception as exc:  # noqa: BLE001 - 失败原因要落到页面
            self._fail(standby, active, exc)

    def _verify_standby(self, role: str, password: str,
                        inject_failure: bool) -> None:
        """用备用账号连库，验证两张业务表都能读能写。

        所有写操作都在同一个事务里并最终 ROLLBACK，验证订单**不落库**。
        """
        params = db.dsn()
        verify_password = "definitely-wrong-password" if inject_failure else password
        conn = psycopg2.connect(
            host=params["host"],
            port=params["port"],
            dbname=params["dbname"],
            user=role,
            password=verify_password,
            connect_timeout=5,
        )
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("SELECT current_user, pg_backend_pid()")
                current_user, pid = cur.fetchone()
                if current_user != role:  # pragma: no cover - 理论不会发生
                    raise RuntimeError(f"连接账号异常: {current_user}")
                # 读验证：两张表都要能 SELECT
                cur.execute(f"SELECT count(*) FROM {db.ORDERS}")
                order_count = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM {db.ORDER_ITEMS}")
                item_count = cur.fetchone()[0]
                # 写验证：两表各 INSERT 一行，事务回滚 -> 不落库
                cur.execute(
                    f"INSERT INTO {db.ORDERS} (order_no, customer) "
                    "VALUES (%s, %s) RETURNING id",
                    (f"ROTATION-CHECK-{pid}", "轮换验证临时单"),
                )
                verify_order_id = cur.fetchone()[0]
                cur.execute(
                    f"INSERT INTO {db.ORDER_ITEMS} "
                    "(order_id, sku, quantity, unit_price) "
                    "VALUES (%s, %s, %s, %s)",
                    (verify_order_id, "ROTATION-CHECK", 1, "0.01"),
                )
                # 回滚前再读一次，确认本事务内写入可见
                cur.execute(
                    f"SELECT count(*) FROM {db.ORDERS} "
                    "WHERE order_no = %s",
                    (f"ROTATION-CHECK-{pid}",),
                )
                if cur.fetchone()[0] != 1:
                    raise RuntimeError("验证事务内写入不可见，写验证失败")
            conn.rollback()  # 关键：验证数据全部回滚，不落库
        finally:
            conn.close()
        with self._lock:
            self._log(
                f"{role} 验证通过：可读 orders（{order_count} 行）/"
                f"order_items（{item_count} 行），两表试写已随事务回滚"
            )

    def _fail(self, standby: str, active: str, exc: Exception) -> None:
        """验证失败：备用账号恢复 NOLOGIN，业务继续使用原账号。"""
        reason = str(exc).strip() or exc.__class__.__name__
        try:
            # 备用账号恢复为 NOLOGIN；它的口令也换成了新口令，
            # 但 NOLOGIN 下无法建连，下次轮换会再生成新口令
            db.set_role_login(standby, False)
        except Exception as inner:  # pragma: no cover - 兜底
            reason = f"{reason}；此外恢复备用账号状态失败: {inner}"
        with self._lock:
            self.state = FAILED
            self.error = reason
            self.last_finished_at = _now()
            self.last_result = "failed"
            # 活动指针从未翻转，确保业务沿用原账号
            self.active_role = active
            self.standby_role = standby
            self._persist_locked()
            self._log(
                f"轮换失败，继续使用原账号 {active}，可重新发起：{reason}"
            )


_STATE_LABELS = {
    IDLE: "空闲",
    VERIFYING: "验证备用账号中",
    SWITCHING: "正在切换",
    SUCCEEDED: "轮换成功",
    FAILED: "失败（沿用原账号）",
}


# ------------------------------------------------------------ 进程级单例
_manager: RotationManager | None = None
_manager_lock = threading.Lock()


def get_manager() -> RotationManager:
    global _manager
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                _manager = RotationManager()
    return _manager


def reset_manager_for_restart(state_dir: str | None = None) -> RotationManager:
    """测试/演示用：丢弃内存中的管理器并重新走一遍启动对账，
    等价于网页服务在同一持久化卷上重启。"""
    global _manager
    with _manager_lock:
        _manager = RotationManager(state_dir=state_dir)
        _manager.bootstrap()
        return _manager
