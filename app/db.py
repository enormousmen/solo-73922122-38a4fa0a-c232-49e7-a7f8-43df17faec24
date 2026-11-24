"""数据库连接与 schema 管理。

两张业务表：
- orders       订单表
- order_items  明细表
迁移要给两张表各加一列可空的 batch_no（批次号）。

账号模型
========
- **管理员账号**（POSTGRES 超级用户，来自 PGUSER/PGPASSWORD）：只用于建库、
  建表、ALTER、角色管理（CREATE/ALTER ROLE、GRANT）等管理动作；
- **业务账号**（app_a / app_b，两个轮流启用的 PostgreSQL ROLE）：订单读写、
  演示读事务等业务连接一律使用当前启用的业务账号，口令只保存在服务端，
  轮换流程由 ``app.rotation`` 管理。
"""
from __future__ import annotations

import os
import time

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql as pg_sql

ORDERS = "orders"
ORDER_ITEMS = "order_items"
BATCH_COLUMN = "batch_no"
TABLES = (ORDERS, ORDER_ITEMS)

# 两个轮流启用的 PostgreSQL 业务账号（ROLE）。
# 任一时刻只有当前账号允许 LOGIN，另一个被置为 NOLOGIN。
ROLE_A = "app_a"
ROLE_B = "app_b"
BUSINESS_ROLES = (ROLE_A, ROLE_B)


def dsn(dbname: str | None = None) -> dict:
    """管理员账号连接参数。"""
    return {
        "host": os.environ.get("PGHOST", "127.0.0.1"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
        "dbname": dbname or os.environ.get("PGDATABASE", "orders_demo"),
        "connect_timeout": 5,
    }


def connect(dbname: str | None = None, autocommit: bool = False):
    """以管理员账号建连（建库/DDL/角色管理使用）。"""
    conn = psycopg2.connect(**dsn(dbname))
    conn.autocommit = autocommit
    return conn


def business_connect(autocommit: bool = False):
    """以当前启用的业务账号建连（订单读写、演示读事务使用）。

    返回 ``(conn, role)``，role 为本次连接实际使用的业务账号名。
    延迟导入 ``rotation`` 以避免循环依赖。
    """
    from .rotation import get_manager

    role, password = get_manager().active_credentials()
    params = dsn()
    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=role,
        password=password,
        connect_timeout=5,
    )
    conn.autocommit = autocommit
    return conn, role


def wait_for_database(timeout: float = 60.0) -> None:
    """容器启动时 db 可能还没就绪，轮询等待。"""
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            conn = connect(dbname="postgres", autocommit=True)
            conn.close()
            return
        except psycopg2.Error as exc:  # pragma: no cover - 启动竞态
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"数据库在 {timeout}s 内未就绪: {last_error}")


def ensure_database() -> None:
    """创建演示数据库（幂等）。"""
    conn = connect(dbname="postgres", autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s",
                        (dsn()["dbname"],))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE {dsn()["dbname"]}')
    finally:
        conn.close()


SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {ORDERS} (
    id          BIGSERIAL PRIMARY KEY,
    order_no    VARCHAR(40) NOT NULL UNIQUE,
    customer    VARCHAR(100) NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'created',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {ORDER_ITEMS} (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES {ORDERS}(id),
    sku         VARCHAR(60) NOT NULL,
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(12, 2) NOT NULL
);
"""

SEED_ORDERS = [
    ("SO-1001", "张三", [
        ("SKU-A100", 2, "19.90"),
        ("SKU-B200", 1, "128.00"),
    ]),
    ("SO-1002", "李四", [
        ("SKU-C300", 5, "9.90"),
    ]),
]


def init_schema(seed: bool = True) -> None:
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            if seed:
                cur.execute(f"SELECT count(*) FROM {ORDERS}")
                if cur.fetchone()[0] == 0:
                    for order_no, customer, items in SEED_ORDERS:
                        cur.execute(
                            f"INSERT INTO {ORDERS} (order_no, customer) "
                            "VALUES (%s, %s) RETURNING id",
                            (order_no, customer),
                        )
                        order_id = cur.fetchone()[0]
                        for sku, qty, price in items:
                            cur.execute(
                                f"INSERT INTO {ORDER_ITEMS} "
                                "(order_id, sku, quantity, unit_price) "
                                "VALUES (%s, %s, %s, %s)",
                                (order_id, sku, qty, price),
                            )
    finally:
        conn.close()


# ---------------------------------------------------------------- 角色管理
def _role_exists(cur, role: str) -> bool:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    return cur.fetchone() is not None


def ensure_business_role(role: str, password: str) -> None:
    """幂等地创建/更新业务账号并授予两表读写所需权限。

    角色口令由调用方（轮换管理器）生成并持有；这里不负责决定哪个账号
    当前允许 LOGIN。
    """
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            if _role_exists(cur, role):
                cur.execute(
                    pg_sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                        pg_sql.Identifier(role), pg_sql.Literal(password)
                    )
                )
            else:
                cur.execute(
                    pg_sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        pg_sql.Identifier(role), pg_sql.Literal(password)
                    )
                )
            grant_business_privileges(cur, role)
    finally:
        conn.close()


def grant_business_privileges(cur, role: str) -> None:
    """授予业务账号在演示库内读写两张业务表与序列的权限。

    reset_database 重建表后也要重新执行（新建的表默认不带旧授权）。
    """
    db_name = dsn()["dbname"]
    cur.execute(
        pg_sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            pg_sql.Identifier(db_name), pg_sql.Identifier(role)
        )
    )
    cur.execute(
        pg_sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
            pg_sql.Identifier(role)
        )
    )
    cur.execute(
        pg_sql.SQL(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
            "public TO {}"
        ).format(pg_sql.Identifier(role))
    )
    cur.execute(
        pg_sql.SQL(
            "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}"
        ).format(pg_sql.Identifier(role))
    )
    # 将来本会话新建的表/序列默认也带授权（演示用，保持幂等）
    cur.execute(
        pg_sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
        ).format(pg_sql.Identifier(role))
    )
    cur.execute(
        pg_sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            "GRANT USAGE, SELECT ON SEQUENCES TO {}"
        ).format(pg_sql.Identifier(role))
    )


def set_role_password(role: str, password: str, login: bool) -> None:
    """管理员重置业务账号口令，并按需设置 LOGIN / NOLOGIN。"""
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            kind = "LOGIN" if login else "NOLOGIN"
            cur.execute(
                pg_sql.SQL(
                    "ALTER ROLE {} WITH {} PASSWORD {}"
                ).format(
                    pg_sql.Identifier(role),
                    pg_sql.SQL(kind),
                    pg_sql.Literal(password),
                )
            )
    finally:
        conn.close()


def set_role_login(role: str, login: bool) -> None:
    """只切换业务账号的 LOGIN / NOLOGIN，不改口令。"""
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            kind = "LOGIN" if login else "NOLOGIN"
            cur.execute(
                pg_sql.SQL("ALTER ROLE {} WITH {}").format(
                    pg_sql.Identifier(role), pg_sql.SQL(kind)
                )
            )
    finally:
        conn.close()


def role_can_login(role: str) -> bool:
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s",
                (role,),
            )
            row = cur.fetchone()
            return bool(row and row[0])
    finally:
        conn.close()


def column_exists(table: str, column: str = BATCH_COLUMN) -> bool:
    if table not in TABLES:
        raise ValueError(f"未知表: {table}")
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_name = %s AND column_name = %s
                """,
                (table, column),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def migration_done() -> bool:
    return all(column_exists(t) for t in TABLES)


def reset_database() -> None:
    """删除两表并重建样例数据，便于反复演示。

    重建后的表是全新对象，业务账号需要重新 GRANT 才能继续读写。
    """
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {ORDER_ITEMS}")
            cur.execute(f"DROP TABLE IF EXISTS {ORDERS}")
            cur.execute(SCHEMA_SQL)
            cur.execute(f"SELECT count(*) FROM {ORDERS}")
            if cur.fetchone()[0] == 0:
                for order_no, customer, items in SEED_ORDERS:
                    cur.execute(
                        f"INSERT INTO {ORDERS} (order_no, customer) "
                        "VALUES (%s, %s) RETURNING id",
                        (order_no, customer),
                    )
                    order_id = cur.fetchone()[0]
                    for sku, qty, price in items:
                        cur.execute(
                            f"INSERT INTO {ORDER_ITEMS} "
                            "(order_id, sku, quantity, unit_price) "
                            "VALUES (%s, %s, %s, %s)",
                            (order_id, sku, qty, price),
                        )
            for role in BUSINESS_ROLES:
                grant_business_privileges(cur, role)
    finally:
        conn.close()


def list_orders() -> list[dict]:
    """读取订单 + 明细。迁移前列不存在，迁移后把批次号一起带出。"""
    conn, _role = business_connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            extra = f", o.{BATCH_COLUMN}" if column_exists(ORDERS) else ""
            cur.execute(
                f"SELECT o.id, o.order_no, o.customer, o.status, "
                f"o.created_at{extra} FROM {ORDERS} o ORDER BY o.id"
            )
            orders = [dict(row) for row in cur.fetchall()]
            item_extra = f", i.{BATCH_COLUMN}" if column_exists(ORDER_ITEMS) else ""
            cur.execute(
                f"SELECT i.id, i.order_id, i.sku, i.quantity, i.unit_price"
                f"{item_extra} FROM {ORDER_ITEMS} i ORDER BY i.id"
            )
            items = [dict(row) for row in cur.fetchall()]
        by_order: dict[int, list[dict]] = {}
        for item in items:
            by_order.setdefault(item["order_id"], []).append(item)
        for order in orders:
            order["items"] = by_order.get(order["id"], [])
        return orders
    finally:
        conn.close()


def insert_order(customer: str, order_no: str | None,
                 items: list[dict], batch_no: str | None) -> dict:
    """试写一笔订单及其明细；迁移后可同时写批次号。"""
    conn, _role = business_connect()
    try:
        with conn.cursor() as cur:
            # order_no 为 NOT NULL UNIQUE：用序列发号而非 max(id)+1，
            # 保证并发下不撞唯一键（未使用的号段会有空洞，业务可接受）。
            if not order_no:
                cur.execute(
                    "SELECT nextval(pg_get_serial_sequence('orders', 'id'))"
                )
                order_no = f"SO-{cur.fetchone()[0]:04d}"

            order_cols = "order_no, customer, status"
            order_placeholders = "%s, %s, %s"
            params: list = [order_no, customer, "created"]
            if column_exists(ORDERS):
                order_cols += f", {BATCH_COLUMN}"
                order_placeholders += ", %s"
                params.append(batch_no)
            cur.execute(
                f"INSERT INTO {ORDERS} ({order_cols}) "
                f"VALUES ({order_placeholders}) RETURNING id",
                params,
            )
            order_id = cur.fetchone()[0]

            has_item_batch = column_exists(ORDER_ITEMS)
            for item in items:
                cols = "order_id, sku, quantity, unit_price"
                vals = "%s, %s, %s, %s"
                iparams: list = [order_id, item["sku"],
                                  int(item["quantity"]), item["unit_price"]]
                if has_item_batch:
                    cols += f", {BATCH_COLUMN}"
                    vals += ", %s"
                    iparams.append(batch_no)
                cur.execute(
                    f"INSERT INTO {ORDER_ITEMS} ({cols}) VALUES ({vals})",
                    iparams,
                )
        conn.commit()
        return {"order_id": order_id, "order_no": order_no}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
