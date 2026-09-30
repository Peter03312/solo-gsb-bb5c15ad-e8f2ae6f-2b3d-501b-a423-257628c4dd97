"""SQLAlchemy 引擎与会话管理。

并发正确性依赖单条事务 + 行锁：
- PostgreSQL：读批次时使用 ``SELECT ... FOR UPDATE``，串行化对同一批次余量的扣减；
- SQLite（本地开发/测试）：所有写事务以 ``BEGIN IMMEDIATED`` 开始，直接升级为
  全库写锁，配合 WAL 与 busy timeout，同样能拒绝并发超用。
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from . import config


class Base(DeclarativeBase):
    pass


_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def is_sqlite(engine: Engine | None = None) -> bool:
    eng = engine or _engine
    return bool(eng is not None and eng.dialect.name == "sqlite")


def init_engine(url: str | None = None) -> Engine:
    """（重新）初始化全局引擎；测试时用内存/临时库覆盖。"""
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
    target = url or config.DATABASE_URL
    sqlite = target.startswith("sqlite")
    if sqlite:
        connect_args = {"timeout": 20, "check_same_thread": False}
        # 静态池让内存库在多线程/多连接间共享同一份数据
        if ":memory:" in target:
            from sqlalchemy.pool import StaticPool

            connect_args["poolclass"] = StaticPool
        _engine = create_engine(target, connect_args=connect_args, future=True)

        @event.listens_for(_engine, "connect")
        def _sqlite_pragmas(dbapi_conn, _record):  # noqa: ANN001
            # 关闭 pysqlite 自带的事务管理，改由 begin 事件显式发起 BEGIN IMMEDIATE
            dbapi_conn.isolation_level = None
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA busy_timeout=20000")
            cur.close()

        # 让 SQLAlchemy 的事务以 BEGIN IMMEDIATE 开始，写事务全局串行
        @event.listens_for(_engine, "begin")
        def _sqlite_begin_immediate(conn):  # noqa: ANN001
            conn.exec_driver_sql("BEGIN IMMEDIATE")
    else:
        _engine = create_engine(
            target,
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=10,
            pool_pre_ping=True,
            future=True,
        )
    _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    return _engine


def get_engine() -> Engine:
    if _engine is None:
        init_engine()
    assert _engine is not None
    return _engine


def init_db() -> None:
    """建表（幂等），应用启动与容器入口都会调用。"""
    # 触发模型注册
    from . import models  # noqa: F401

    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    _upgrade_columns(engine)


def _upgrade_columns(engine: Engine) -> None:
    """轻量幂等列升级：既有库由旧版本建表时缺少后续新增列，此处按需补齐。

    PostgreSQL 与 SQLite 均支持 ``ADD COLUMN ... DEFAULT FALSE``（存量行回填），
    无需手工迁移；新库由 ``create_all`` 直接建出完整结构。
    """
    inspector = inspect(engine)
    if "productions" not in inspector.get_table_names():
        return
    existing = {col["name"] for col in inspector.get_columns("productions")}
    statements = []
    if "is_cancelled" not in existing:
        default_false = "0" if engine.dialect.name == "sqlite" else "FALSE"
        statements.append(
            f"ALTER TABLE productions ADD COLUMN is_cancelled BOOLEAN "
            f"NOT NULL DEFAULT {default_false}"
        )
    if statements:
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))


@contextmanager
def session_scope() -> Iterator[Session]:
    """业务事务边界：业务异常回滚，正常结束提交。"""
    if _SessionLocal is None:
        init_engine()
    assert _SessionLocal is not None
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_session() -> Iterator[Session]:
    """FastAPI 依赖。"""
    with session_scope() as session:
        yield session


def ping() -> None:
    """供容器入口等待数据库就绪。"""
    with get_engine().connect() as conn:
        conn.execute(text("SELECT 1"))
        conn.commit()
