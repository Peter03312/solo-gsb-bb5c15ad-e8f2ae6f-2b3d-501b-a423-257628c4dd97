"""业务逻辑层。所有写操作都在单个事务内完成，校验失败整体回滚。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from . import errors
from .models import (
    Batch,
    BatchReview,
    IdempotencyKey,
    Production,
    ProductionCancellation,
    ProductionInput,
)
from .schemas import (
    CancelProductionIn,
    ProductionIn,
    RawBatchIn,
    ReviewIn,
    SuspectIn,
)

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


def _acquire_write_lock(session: Session) -> None:
    """全部写事务（投料 / 污染标记 / 复核 / 撤销生产单）共用的串行化锁。

    PostgreSQL：对 batches 取 SHARE ROW EXCLUSIVE 表锁。它与任意另一个写事务
    持有的同类表锁互斥（彼此只能排队），但不阻塞普通 SELECT，从而把所有写操作
    归并为单一全序；各事务随后再按编号升序取行锁——持锁顺序一致，不会出现
    “A 等 B 的行锁、B 在表锁队列中等 A”的死锁环。
    - 投料之间：行锁仍负责同批次余量的精确串行；
    - 标记/复核/撤销与投料：闭包读取/谱系校验不会看到并发写到一半的谱系边，
      隔离批次绝不会被投料、撤销绝不会与并发投料互相超用。
    SQLite：BEGIN IMMEDIATE 已提供全库写串行化，无需额外处理。
    """
    if session.get_bind().dialect.name == "postgresql":
        session.execute(text("LOCK TABLE batches IN SHARE ROW EXCLUSIVE MODE"))


# 向后兼容的旧名称（QC 操作的串行化锁即通用写锁）
_acquire_qc_lock = _acquire_write_lock


def submit_production(session: Session, data: ProductionIn) -> Production:
    """提交一次生产：校验全部通过后才扣减余量并创建产出批次。

    校验顺序（任一失败整笔拒绝、不改变余量）：
    1. 投入明细编号不重复；
    2. 每个来源批次存在、未撤销、单位一致、未隔离；
    3. 产出编号未被占用；
    4. 每笔用量为正且不超过该批次余量；
    5. 产出总量不超过投入总量。

    并发：事务一开始即取全局写锁（见 ``_acquire_write_lock``），与撤销、污染
    标记、复核事务互斥，随后按编号升序加来源行锁；撤销事务返还余量、校验
    “产出未被后续投料”期间不可能有并发投料插入。
    """
    _acquire_write_lock(session)

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

    # 已撤销生产单的产出批次不得再投料（撤销边不属有效谱系）
    cancelled_nos = _cancelled_output_nos(session, source_nos)
    for item in data.inputs:
        if item.batch_no in cancelled_nos:
            raise errors.output_cancelled(item.batch_no)

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


# ---------------------------------------------------------------- 生产单撤销


@dataclass
class CancelResult:
    """撤销事务结果：原单据、撤销信息与各来源返还明细。"""

    production: Production
    output: Batch
    reason: str
    cancelled_at: datetime
    returns: list[tuple[Batch, Decimal]]  # (来源批次, 返还量)，返还后的余量挂在批次上


def cancel_production(session: Session, output_no: str, data: CancelProductionIn) -> CancelResult:
    """撤销一张生产单并在同一事务内返还各来源的原投料量。

    仅当产出批次同时满足以下条件才允许撤销（任一不符明确拒绝、不改变库存）：
    1. 批次编号存在且确为某张生产单的产出；
    2. 该生产单尚未撤销（重复撤销拒绝）；
    3. 产出当前非疑似（``is_suspect`` 为假）；
    4. 该产出不存在质检复核记录；
    5. 该产出从未被后续*有效*生产投料（撤销单的引用不算）。

    即使来源批次已隔离，撤销也照常把各来源的原投料量加回其余量
    （返还是冲销本单的错误扣减，不是投料/放行）。撤销后：产出余量置 0 且
    清除隔离标记（其谱系祖先边已随本单失效），产出不得再投料，本单投料边
    退出有效谱系与疑似传播。

    并发：与投料/标记/复核共用全局写锁（``_acquire_write_lock``），再按编号
    升序锁定“产出 + 全部来源”，并对生产单行加 ``FOR UPDATE`` 兜住并发双撤，
    整体结果等价于某一串行顺序。
    """
    _acquire_write_lock(session)

    output = _lock_batch(session, output_no)
    if output is None:
        raise errors.batch_not_found(output_no)

    prod_stmt = select(Production).where(Production.output_no == output_no)
    if session.get_bind().dialect.name == "postgresql":
        prod_stmt = prod_stmt.with_for_update()
    production = session.execute(
        prod_stmt.options(selectinload(Production.inputs))
    ).scalar_one_or_none()
    if production is None:
        raise errors.batch_not_production(output_no)
    if production.is_cancelled:
        raise errors.production_already_cancelled(output_no)

    # 当前非疑似
    if output.is_suspect:
        raise errors.batch_suspect(output_no)

    # 无复核记录
    reviewed = session.execute(
        select(BatchReview.id).where(BatchReview.batch_no == output_no).limit(1)
    ).first()
    if reviewed is not None:
        raise errors.production_has_review(output_no)

    # 从未被后续有效生产投料（撤销边不计）
    consumers = _active_consumers_of(session, output_no)
    if consumers:
        raise errors.production_has_downstream(output_no, consumers)

    # 锁定全部来源批次（编号升序，与投料事务同一加锁序）后返还原投料量。
    # 来源即便已隔离也照常返还——撤销只冲销本单扣减，不改变其隔离/疑似状态。
    source_nos = sorted({pi.source_no for pi in production.inputs})
    locked_sources = _lock_batches_in_order(session, source_nos)

    returns: list[tuple[Batch, Decimal]] = []
    for pi in sorted(production.inputs, key=lambda x: x.id):
        source = locked_sources[pi.source_no]
        source.remaining_qty += pi.qty
        returns.append((source, pi.qty))

    # 产出作废：余量清零（不得再投料）；本单投料边已退出有效谱系，该产出不再
    # 有任何有效祖先，隔离标记随之清除（疑似标记已由上面的校验保证为假）。
    output.remaining_qty = Decimal("0")
    output.is_quarantined = False

    production.is_cancelled = True
    # 与 server_default 的 func.now() 保持一致：使用无时区标注的 UTC 时刻，
    # 响应序列化风格统一（不加 'Z' 后缀）。
    cancelled_at = datetime.now(timezone.utc).replace(tzinfo=None)
    session.add(
        ProductionCancellation(
            production_id=production.id,
            output_no=output_no,
            reason=data.reason,
            cancelled_at=cancelled_at,
        )
    )
    session.flush()
    return CancelResult(
        production=production,
        output=output,
        reason=data.reason,
        cancelled_at=cancelled_at,
        returns=returns,
    )


def get_production_detail(session: Session, output_no: str) -> tuple[Production, Batch]:
    """按产出批次编号加载生产单（含投料明细、撤销记录）与产出批次（取单位）。

    编号不存在 -> SOURCE_NOT_FOUND(404)；批次存在但不是生产产出
    -> NOT_PRODUCTION_OUTPUT(409)。
    """
    output = session.get(Batch, output_no)
    if output is None:
        raise errors.batch_not_found(output_no)
    production = session.execute(
        select(Production)
        .options(
            selectinload(Production.inputs),
            selectinload(Production.cancellation),
        )
        .where(Production.output_no == output_no)
    ).scalar_one_or_none()
    if production is None:
        raise errors.batch_not_production(output_no)
    return production, output


# ---------------------------------------------------------------- 谱系与污染

# 边方向：source -> output（原料指向产出）。
# 已撤销生产单的投料边（productions.is_cancelled = TRUE）不属有效谱系：
# 不进入谱系查询，也不参与疑似污染的后代传播 / 祖先谓词。
_EDGE_CTE = """
SELECT pi.source_no AS parent_no, p.output_no AS child_no
FROM production_inputs pi
JOIN productions p ON p.id = pi.production_id
WHERE p.is_cancelled = FALSE
"""


def _cancelled_output_nos(session: Session, batch_nos: list[str]) -> set[str]:
    """给定批次编号中，属于“已撤销生产单产出”的子集。"""
    if not batch_nos:
        return set()
    rows = session.execute(
        select(Production.output_no).where(
            Production.is_cancelled.is_(True),
            Production.output_no.in_(batch_nos),
        )
    ).all()
    return {row[0] for row in rows}


def is_cancelled_batch(session: Session, batch_no: str) -> bool:
    """该批次是否为某张已撤销生产单的产出（供查询接口派生撤销标记）。"""
    return batch_no in _cancelled_output_nos(session, [batch_no])


def _active_consumers_of(session: Session, batch_no: str) -> list[str]:
    """求当前把该批次作为来源的全部*有效*（未撤销）生产单的产出编号。

    撤销仅允许“从未被后续生产投料”的产出，因此命中的有效消费方即拒绝原因；
    撤销单之间的引用不算后续投料。
    """
    rows = session.execute(
        select(Production.output_no)
        .join(ProductionInput, ProductionInput.production_id == Production.id)
        .where(
            Production.is_cancelled.is_(False),
            ProductionInput.source_no == batch_no,
        )
        .order_by(Production.output_no)
    ).all()
    return [r[0] for r in rows]


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
            # 有效谱系只含未撤销边；撤销产出不在上游/下游节点集合中
            "is_cancelled": False,
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
            Production.is_cancelled.is_(False),
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
