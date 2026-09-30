"""业务逻辑层。所有写操作都在单个事务内完成，校验失败整体回滚。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from . import errors
from .models import Batch, BatchReview, IdempotencyKey, Production, ProductionInput
from .schemas import ProductionIn, RawBatchIn, ReviewIn, SuspectIn

# ---------------------------------------------------------------- 幂等支持


def _canonical_hash(payload: dict[str, Any]) -> str:
    """对请求体做规范序列化后取 sha256；Decimal 用原始标量文本表示。"""

    def default(obj: object) -> object:
        if isinstance(obj, Decimal):
            return f"decimal:{normalize_decimal(obj)}"
        raise TypeError(f"不可序列化的类型 {type(obj)!r}")

    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=default)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def normalize_decimal(value: Decimal) -> str:
    """1 与 1.00 视为同一内容；去掉多余尾零。"""
    return format(value.normalize(), "f")


def idempotent(
    session: Session,
    *,
    method: str,
    path: str,
    key: str | None,
    payload: dict[str, Any],
    action: Callable[[], tuple[int, Any]],
) -> tuple[int, Any]:
    """以幂等键执行动作。

    - 无 key：直接执行（查询类接口不需要）；
    - 同 key + 同内容（含重试）：返回首次固化的状态码与响应；
    - 同 key + 异内容：409 冲突；
    - 失败请求不落幂等记录，允许客户端修正后以同 key 重试。
    """
    if not key:
        status, body = action()
        return status, body

    request_hash = _canonical_hash(payload)
    stmt = select(IdempotencyKey).where(
        IdempotencyKey.method == method,
        IdempotencyKey.path == path,
        IdempotencyKey.key == key,
    )
    existing = session.execute(stmt).scalar_one_or_none()
    if existing is not None:
        if existing.request_hash != request_hash:
            raise AppError_conflict(key)
        return existing.status_code, json.loads(existing.response_body)

    status, body = action()

    record = IdempotencyKey(
        method=method,
        path=path,
        key=key,
        request_hash=request_hash,
        status_code=status,
        response_body=json.dumps(body, ensure_ascii=False, default=str),
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        # 并发下另一请求已抢先登记该 key
        existing = session.execute(stmt).scalar_one_or_none()
        if existing is not None:
            if existing.request_hash != request_hash:
                raise AppError_conflict(key)
            return existing.status_code, json.loads(existing.response_body)
        raise
    return status, body


def AppError_conflict(key: str) -> errors.AppError:
    return errors.AppError(
        "IDEMPOTENCY_CONFLICT",
        f"幂等键 {key!r} 已用于不同的请求内容，异内容冲突",
        409,
    )


# ---------------------------------------------------------------- 批次操作


def register_raw_batch(session: Session, data: RawBatchIn) -> Batch:
    """登记一个原料批次。编号不得复用。"""
    existing = session.get(Batch, data.batch_no)
    if existing is not None:
        raise errors.batch_exists(data.batch_no)

    batch = Batch(
        batch_no=data.batch_no,
        unit=data.unit,
        initial_qty=data.quantity,
        remaining_qty=data.quantity,
        note=data.note,
    )
    session.add(batch)
    try:
        session.flush()
    except IntegrityError:
        raise errors.batch_exists(data.batch_no) from None
    return batch


def _lock_batch(session: Session, batch_no: str) -> Batch | None:
    """读取批次并加行锁（PostgreSQL FOR UPDATE；SQLite 被写事务锁覆盖）。"""
    stmt = select(Batch).where(Batch.batch_no == batch_no)
    if session.get_bind().dialect.name == "postgresql":
        stmt = stmt.with_for_update()
    return session.execute(stmt).scalar_one_or_none()


def _lock_batches_in_order(
    session: Session, batch_nos: list[str]
) -> dict[str, Batch]:
    """按编号排序后逐个锁定批次。

    全局统一的加锁顺序（编号升序）保证并发生产事务之间不会出现
    “互相持有对方等待的行锁”式死锁（PostgreSQL FOR UPDATE；
    SQLite 写事务已被 BEGIN IMMEDIATE 全局串行化，FOR UPDATE 为空操作）。
    """
    ordered = sorted(set(batch_nos))
    locked: dict[str, Batch] = {}
    for no in ordered:
        stmt = select(Batch).where(Batch.batch_no == no)
        if session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update()
        batch = session.execute(stmt).scalar_one_or_none()
        if batch is not None:
            locked[no] = batch
    return locked


def _acquire_qc_lock(session: Session) -> None:
    """污染标记/复核等 QC 写事务的串行化锁。

    PostgreSQL：对 batches 取 SHARE ROW EXCLUSIVE 表锁。它与生产事务的
    ROW SHARE（SELECT ... FOR UPDATE）/ ROW EXCLUSIVE（写入）冲突，也与
    其它 QC 事务互斥——使“标记/复核”与“投料”严格串行，既杜绝隔离批次被
    投料，也避免“先锁 root、再锁后代”与生产的排序行锁之间形成死锁环。
    QC 操作低频，粗粒度表锁代价可接受。
    SQLite：BEGIN IMMEDIATE 已提供全库写串行化，无需额外处理。
    """
    if session.get_bind().dialect.name == "postgresql":
        session.execute(text("LOCK TABLE batches IN SHARE ROW EXCLUSIVE MODE"))


def submit_production(session: Session, data: ProductionIn) -> Production:
    """提交一次生产：校验全部通过后才扣减余量并创建产出批次。

    校验顺序（任一失败整笔拒绝、不改变余量）：
    1. 投入明细编号不重复；
    2. 每个来源批次存在、单位一致、未隔离；
    3. 产出编号未被占用；
    4. 每笔用量为正且不超过该批次余量；
    5. 产出总量不超过投入总量。
    """
    source_nos = [item.batch_no for item in data.inputs]
    if len(set(source_nos)) != len(source_nos):
        raise errors.AppError(
            "DUPLICATE_INPUT",
            "同一生产单中来源批次不得重复",
            422,
        )

    sources: dict[str, Batch] = {}
    total_input = Decimal("0")
    # 先按编号排序一次性锁定所有来源批次，避免并发事务交叉加锁死锁
    # （PostgreSQL 行锁；SQLite 为立即写锁）
    locked = _lock_batches_in_order(session, source_nos)
    for no in source_nos:
        batch = locked.get(no)
        if batch is None:
            raise errors.batch_not_found(no)
        sources[no] = batch

    for item in data.inputs:
        batch = sources[item.batch_no]
        if batch.unit != data.unit:
            raise errors.unit_mismatch(item.batch_no, batch.unit, data.unit)
        if batch.is_quarantined:
            raise errors.batch_quarantined(item.batch_no)

    if session.get(Batch, data.output_batch_no) is not None:
        raise errors.batch_exists(data.output_batch_no)

    for item in data.inputs:
        batch = sources[item.batch_no]
        if item.quantity > batch.remaining_qty:
            raise errors.insufficient(
                item.batch_no, item.quantity, batch.remaining_qty
            )
        total_input += item.quantity

    if data.output_quantity > total_input:
        raise errors.AppError(
            "OUTPUT_EXCEEDS_INPUT",
            f"产出量 {normalize_decimal(data.output_quantity)} 超过投入总量 "
            f"{normalize_decimal(total_input)}",
            422,
            details={
                "output_quantity": normalize_decimal(data.output_quantity),
                "total_input": normalize_decimal(total_input),
            },
        )

    output = Batch(
        batch_no=data.output_batch_no,
        unit=data.unit,
        initial_qty=data.output_quantity,
        remaining_qty=data.output_quantity,
    )
    session.add(output)
    try:
        session.flush()  # 让产出批次主键可用，并尽早暴露编号冲突
    except IntegrityError:
        raise errors.batch_exists(data.output_batch_no) from None

    production = Production(
        output_no=output.batch_no, output_qty=data.output_quantity
    )
    session.add(production)
    session.flush()

    for item in data.inputs:
        batch = sources[item.batch_no]
        batch.remaining_qty -= item.quantity
        session.add(
            ProductionInput(
                production_id=production.id,
                source_no=batch.batch_no,
                qty=item.quantity,
            )
        )

    session.flush()
    # 重新加载聚合，便于响应组装
    return session.execute(
        select(Production)
        .options(selectinload(Production.inputs))
        .where(Production.id == production.id)
    ).scalar_one()


# ---------------------------------------------------------------- 谱系与污染

# 边方向：source -> output（原料指向产出）
_EDGE_CTE = """
SELECT pi.source_no AS parent_no, p.output_no AS child_no
FROM production_inputs pi JOIN productions p ON p.id = pi.production_id
"""


def _descendant_rows(session: Session, batch_no: str) -> list[dict[str, Any]]:
    """递归求全部后代（直接/间接使用该批次的产出），带层数。"""
    sql = f"""
    WITH RECURSIVE walk(batch_no, depth, path) AS (
        SELECT :root, 0, :root
        UNION
        SELECT e.child_no, w.depth + 1, w.path || '>' || e.child_no
        FROM walk w JOIN ({_EDGE_CTE}) e ON e.parent_no = w.batch_no
    )
    SELECT batch_no, depth FROM walk WHERE depth > 0 ORDER BY depth, batch_no
    """
    return [dict(r._mapping) for r in session.execute(text(sql), {"root": batch_no})]


def _ancestor_rows(session: Session, batch_no: str) -> list[dict[str, Any]]:
    """递归求全部祖先（直接/间接被其消耗的原料），带层数。"""
    sql = f"""
    WITH RECURSIVE walk(batch_no, depth) AS (
        SELECT :root, 0
        UNION
        SELECT e.parent_no, w.depth + 1
        FROM walk w JOIN ({_EDGE_CTE}) e ON e.child_no = w.batch_no
    )
    SELECT batch_no, depth FROM walk WHERE depth > 0 ORDER BY depth, batch_no
    """
    return [dict(r._mapping) for r in session.execute(text(sql), {"root": batch_no})]


def _load_batches_map(session: Session, nos: list[str]) -> dict[str, Batch]:
    if not nos:
        return {}
    rows = session.execute(select(Batch).where(Batch.batch_no.in_(nos))).scalars().all()
    return {b.batch_no: b for b in rows}


def _lock_descendant_closure(
    session: Session, root_no: str
) -> tuple[Batch | None, list[str], dict[str, Batch]]:
    """在固定点谱系上加载 root 及其全部后代。

    调用方必须先经 ``_acquire_qc_lock`` 与并发生产事务串行化，因此此处读到的
    谱系边与批次状态在本事务内稳定（PG 上表锁排除了并发投料，SQLite 上
    BEGIN IMMEDIATE 全库串行），不存在边被并发追加的问题。

    返回 (root 批次[可能不存在], 后代编号[depth 序], 批次映射)。
    """
    root = _lock_batch(session, root_no)
    if root is None:
        return None, [], {}

    rows = _descendant_rows(session, root_no)
    descendant_nos = [r["batch_no"] for r in rows]
    targets = _load_batches_map(session, descendant_nos)
    return root, descendant_nos, {root_no: root, **targets}


# 判断给定批次是否应隔离（自身疑似或任一祖先疑似）：
# 祖先部分以目标批次为根沿 source -> output 反向递归（严格祖先），
# 再并上目标自身；谱系边以同事务内可见数据为准，谱系为无环 DAG。
_ANCESTOR_SUSPECT_SQL = f"""
WITH RECURSIVE anc(batch_no) AS (
    SELECT e.parent_no
    FROM ({_EDGE_CTE}) e
    WHERE e.child_no = :root
    UNION
    SELECT e.parent_no FROM anc a
    JOIN ({_EDGE_CTE}) e ON e.child_no = a.batch_no
)
SELECT EXISTS(
    SELECT 1 FROM batches s
    WHERE s.is_suspect AND (
        s.batch_no = :root OR s.batch_no IN (SELECT batch_no FROM anc)
    )
)
"""


def _suspect_closure_map(
    session: Session, batch_nos: list[str]
) -> dict[str, bool]:
    """对每个目标批次求：自身疑似，或任一祖先疑似（是否应隔离）。"""
    return {
        no: bool(session.execute(text(_ANCESTOR_SUSPECT_SQL), {"root": no}).scalar_one())
        for no in batch_nos
    }


def lineage(session: Session, batch_no: str) -> dict[str, Any]:
    root = session.get(Batch, batch_no)
    if root is None:
        raise errors.batch_not_found(batch_no)

    up_rows = _ancestor_rows(session, batch_no)
    down_rows = _descendant_rows(session, batch_no)

    up_map = _load_batches_map(session, [r["batch_no"] for r in up_rows])
    down_map = _load_batches_map(session, [r["batch_no"] for r in down_rows])

    def node(r: dict[str, Any], b: Batch) -> dict[str, Any]:
        return {
            "batch_no": b.batch_no,
            "unit": b.unit,
            "remaining_qty": b.remaining_qty,
            "is_suspect": b.is_suspect,
            "is_quarantined": b.is_quarantined,
            "depth": r["depth"],
        }

    upstream = [node(r, up_map[r["batch_no"]]) for r in up_rows if r["batch_no"] in up_map]
    downstream = [
        node(r, down_map[r["batch_no"]]) for r in down_rows if r["batch_no"] in down_map
    ]

    involved = set(up_map) | set(down_map) | {batch_no}
    edges = session.execute(
        select(ProductionInput.source_no, Production.output_no)
        .join(Production, Production.id == ProductionInput.production_id)
        .where(
            ProductionInput.source_no.in_(involved),
            Production.output_no.in_(involved),
        )
    ).all()
    return {
        "batch_no": batch_no,
        "upstream": upstream,
        "downstream": downstream,
        "edges": [{"from": s, "to": o} for s, o in edges],
    }


def mark_suspect(session: Session, data: SuspectIn) -> dict[str, Any]:
    """标记批次疑似污染，并隔离其全部后代产出批次。

    混料规则：一个产出若混有受污染来源（直接或间接），该产出整体隔离；
    隔离批次不得再投料（提交生产时拦截）。

    并发：先取 QC 串行化锁（PG 表锁 / SQLite 全库写锁），再读取后代闭包
    并写状态，与生产、复核事务整体等价于某一串行顺序。
    """
    _acquire_qc_lock(session)
    batch, descendant_nos, locked = _lock_descendant_closure(session, data.batch_no)
    if batch is None:
        raise errors.batch_not_found(data.batch_no)

    previously_quarantined = {no for no, b in locked.items() if b.is_quarantined}

    batch.is_suspect = True
    batch.is_quarantined = True
    if data.note:
        batch.note = data.note

    for no in descendant_nos:
        target = locked[no]
        target.is_quarantined = True

    session.flush()
    newly = sorted(set(descendant_nos) - previously_quarantined)
    return {
        "source": data.batch_no,
        "is_suspect": True,
        "affected_batch_nos": descendant_nos,
        "newly_quarantined": newly,
    }


def review_suspect(session: Session, data: ReviewIn) -> dict[str, Any]:
    """质检复核确认误报：撤销批次疑似标记，并重算自身及全部后代的隔离状态。

    规则：
    - 只接受当前仍疑似的批次；编号不存在 -> SOURCE_NOT_FOUND(404)，
      已撤销或从未疑似 -> BATCH_NOT_SUSPECT(409)，均不改变任何状态；
    - 重算范围为该批次及其全部后代：自身或任一祖先仍疑似则继续隔离，否则解除；
    - 混料后代若还有另一条疑似来源（祖先），不得因本来源撤销而解除隔离。

    并发：先取 QC 串行化锁，再按与 mark_suspect 相同的固定点闭包读取，
    与生产事务的来源行锁互斥，保证复核 / 污染标记 / 投料并发时等价于
    某一串行顺序；隔离批次在任一串行结局下都不会被投料。
    """
    _acquire_qc_lock(session)
    batch, descendant_nos, locked = _lock_descendant_closure(session, data.batch_no)
    if batch is None:
        raise errors.batch_not_found(data.batch_no)
    if not batch.is_suspect:
        raise errors.batch_not_suspect(data.batch_no)

    targets = [data.batch_no, *descendant_nos]

    # 撤销自身疑似标记并固化复核说明（同一批次可能经历 标记→撤销→再标记→再撤销，
    # 复用已有复核行更新说明，避免唯一约束冲突）
    batch.is_suspect = False
    review = session.execute(
        select(BatchReview).where(BatchReview.batch_no == data.batch_no)
    ).scalar_one_or_none()
    if review is None:
        session.add(
            BatchReview(batch_no=data.batch_no, review_note=data.review_note)
        )
    else:
        review.review_note = data.review_note
        review.reviewed_at = func.now()

    # 重算：自身疑似或任一祖先疑似 -> 隔离；否则解除。
    # 注意：session.execute(text()) 不会触发 ORM autoflush，必须显式 flush，
    # 让下面的原始 SQL 谓词读到本事务刚撤销的 is_suspect 状态。
    session.flush()
    should_quarantine = _suspect_closure_map(session, targets)

    released: list[str] = []
    still: list[str] = []
    for no in targets:
        target = locked[no]
        quarantined = should_quarantine.get(no, False)
        if target.is_quarantined and not quarantined:
            released.append(no)
        target.is_quarantined = quarantined
        if quarantined:
            still.append(no)

    session.flush()
    released.sort()
    # still 中的第一个即复核批次自身（撤销后通常已解除；仍疑似祖先时会保留）
    still.sort()
    return {
        "reviewed_batch_no": data.batch_no,
        "is_suspect": False,
        "review_note": data.review_note,
        "affected_batch_nos": descendant_nos,
        "released_batch_nos": released,
        "still_quarantined": still,
    }
