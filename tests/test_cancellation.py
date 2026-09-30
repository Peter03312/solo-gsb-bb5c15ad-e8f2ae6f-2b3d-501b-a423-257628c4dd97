"""生产单撤销测试：返还库存、撤销条件、谱系剔除、幂等、并发串行等价。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from app import service
from app.db import session_scope
from app.errors import AppError
from app.models import Batch, Production
from app.schemas import (
    CancelProductionIn,
    ProductionIn,
    RawBatchIn,
    SuspectIn,
)


def _raw(client, batch_no, qty, unit="kg", key=None, note=None):
    return client.post(
        "/raw-batches",
        json={"batch_no": batch_no, "unit": unit, "quantity": qty, "note": note},
        headers={"Idempotency-Key": key or f"key-{batch_no}"},
    )


def _produce(client, out_no, inputs, out_qty=None, unit="kg", key=None):
    total = sum((Decimal(str(q)) for _, q in inputs), Decimal(0))
    return client.post(
        "/productions",
        json={
            "output_batch_no": out_no,
            "unit": unit,
            "output_quantity": str(out_qty if out_qty is not None else total),
            "inputs": [{"batch_no": b, "quantity": str(q)} for b, q in inputs],
        },
        headers={"Idempotency-Key": key or f"key-{out_no}"},
    )


def _cancel(client, output_no, reason="录入错误，撤销", key=None):
    return client.post(
        f"/productions/{output_no}/cancellation",
        json={"reason": reason},
        headers={"Idempotency-Key": key or f"cancel-{output_no}"},
    )


# ------------------------------------------------------------- 基本撤销与返还


def test_cancel_returns_original_input_quantities(client):
    _raw(client, "RM-A", "100")
    _raw(client, "RM-B", "50")
    r = _produce(client, "MIX-1", [("RM-A", "40"), ("RM-B", "20")], out_qty=55)
    assert r.status_code == 201, r.text
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 60.0
    assert client.get("/batches/RM-B").json()["remaining_qty"] == 30.0

    r = _cancel(client, "MIX-1", "产出量录错，整单撤销")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["output_batch_no"] == "MIX-1"
    assert body["status"] == "cancelled"
    assert body["reason"] == "产出量录错，整单撤销"
    assert "cancelled_at" in body and body["cancelled_at"]
    # 返还明细：按来源编号排序，各来源返还原投料量并给出返还后余量
    assert body["returns"] == [
        {"source_batch_no": "RM-A", "returned_quantity": 40.0, "remaining_after": 100.0},
        {"source_batch_no": "RM-B", "returned_quantity": 20.0, "remaining_after": 50.0},
    ]

    assert client.get("/batches/RM-A").json()["remaining_qty"] == 100.0
    assert client.get("/batches/RM-B").json()["remaining_qty"] == 50.0
    # 作废产出余量为 0，且标记为已撤销
    out = client.get("/batches/MIX-1").json()
    assert out["remaining_qty"] == 0.0
    assert out["is_cancelled"] is True
    assert out["is_quarantined"] is False

    # 未撤销批次 is_cancelled 字段存在且为 false（向后兼容）
    assert client.get("/batches/RM-A").json()["is_cancelled"] is False


def test_cancel_returns_to_quarantined_source_too(client):
    """即使来源批次已隔离，也须在同一事务内返还原投料量。"""
    _raw(client, "RM-A", "100")
    _raw(client, "RM-B", "100")
    _produce(client, "MIX-1", [("RM-A", "40"), ("RM-B", "60")], key="1")
    # RM-B 另被标记疑似并隔离（MIX-1 也随之隔离，但 MIX-1 自身非疑似）
    client.post(
        "/contamination",
        json={"batch_no": "RM-B"},
        headers={"Idempotency-Key": "c1"},
    )
    assert client.get("/batches/RM-B").json()["is_quarantined"] is True
    assert client.get("/batches/MIX-1").json()["is_suspect"] is False

    r = _cancel(client, "MIX-1", "录错撤销")
    assert r.status_code == 200, r.text
    # 两个来源都返还，包括已隔离的 RM-B
    after = {line["source_batch_no"]: line["remaining_after"] for line in r.json()["returns"]}
    assert after == {"RM-A": 100.0, "RM-B": 100.0}
    assert client.get("/batches/RM-B").json()["remaining_qty"] == 100.0
    # 来源隔离状态不因其被返还而改变（不是放行）
    assert client.get("/batches/RM-B").json()["is_quarantined"] is True
    mix = client.get("/batches/MIX-1").json()
    assert mix["is_cancelled"] is True and mix["is_quarantined"] is False


def test_cancelled_output_cannot_be_consumed(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    assert _cancel(client, "MIX-1").status_code == 200

    r = _produce(client, "FG-1", [("MIX-1", "5")], key="2")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "BATCH_CANCELLED"
    # 被拒绝后无副作用
    assert client.get("/batches/MIX-1").json()["remaining_qty"] == 0.0
    assert client.get("/batches/FG-1").status_code == 404
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 100.0


def test_cancelled_edges_leave_effective_lineage(client):
    _raw(client, "RM-A", "100")
    _raw(client, "RM-B", "100")
    _produce(client, "MIX-1", [("RM-A", "40"), ("RM-B", "60")], key="1")
    _produce(client, "FG-1", [("MIX-1", "50")], key="2")
    # 撤销 RM-A -> MIX-1 这张单：FG-1 此前已投料 MIX-1，故 MIX-1 不可撤销；
    # 先验证“被后续投料”拒绝，再撤销链条末端后验证谱系剔除
    r = _cancel(client, "MIX-1", key="c-mix")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "PRODUCTION_HAS_DOWNSTREAM"
    assert r.json()["error"]["details"]["consumers"] == ["FG-1"]
    # 拒绝不改库存：MIX-1 已被 FG-1 消耗 50，余量 50
    assert client.get("/batches/MIX-1").json()["remaining_qty"] == 50.0
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 60.0

    # 撤销末端 FG-1（返还 MIX-1），其边退出有效谱系
    assert _cancel(client, "FG-1").status_code == 200
    assert client.get("/batches/MIX-1").json()["remaining_qty"] == 100.0
    lin = client.get("/batches/MIX-1/lineage").json()
    assert [n["batch_no"] for n in lin["downstream"]] == []
    assert ("MIX-1", "FG-1") not in {(e["from"], e["to"]) for e in lin["edges"]}

    # FG-1 仍可按编号查询，但谱系查询里不再作为节点出现
    assert client.get("/batches/FG-1").json()["is_cancelled"] is True
    lin_a = client.get("/batches/RM-A/lineage").json()
    assert "FG-1" not in {n["batch_no"] for n in lin_a["downstream"]}


def test_cancelled_edges_do_not_propagate_contamination(client):
    _raw(client, "RM-A", "100")
    _raw(client, "RM-B", "100")
    _raw(client, "RM-C", "100")
    _produce(client, "MIX-1", [("RM-A", "40"), ("RM-B", "60")], key="1")
    # 撤销前先安排一条有效的新边 RM-A/RM-C -> MIX-2
    _produce(client, "MIX-2", [("RM-A", "10"), ("RM-C", "10")], key="2")
    # 撤销 MIX-1（RM-A/RM-B 余量返还）
    assert _cancel(client, "MIX-1").status_code == 200
    # 撤销后再标记 RM-A 疑似：只沿有效边传播到 MIX-2，不得沿已撤销边到 MIX-1
    r = client.post(
        "/contamination",
        json={"batch_no": "RM-A"},
        headers={"Idempotency-Key": "cont-1"},
    )
    assert r.status_code == 200
    assert r.json()["affected_batch_nos"] == ["MIX-2"]
    assert r.json()["newly_quarantined"] == ["MIX-2"]
    mix = client.get("/batches/MIX-1").json()
    assert mix["is_cancelled"] is True
    assert mix["is_quarantined"] is False
    assert mix["is_suspect"] is False
    assert client.get("/batches/RM-B").json()["is_quarantined"] is False
    assert client.get("/batches/MIX-2").json()["is_quarantined"] is True

    # 有效谱系：RM-A 下游只剩 MIX-2，已撤销的 MIX-1 及其边均不出现
    lin = client.get("/batches/RM-A/lineage").json()
    assert {n["batch_no"] for n in lin["downstream"]} == {"MIX-2"}
    assert ("RM-A", "MIX-1") not in {(e["from"], e["to"]) for e in lin["edges"]}
    assert ("RM-A", "MIX-2") in {(e["from"], e["to"]) for e in lin["edges"]}


def test_chain_cancel_then_reuse_balances(client):
    """撤销末端 → 撤销上游：逐步返还，库存一致。"""
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "60")], key="1")
    _produce(client, "FG-1", [("MIX-1", "30")], key="2")
    assert _cancel(client, "FG-1").status_code == 200
    assert client.get("/batches/MIX-1").json()["remaining_qty"] == 60.0
    assert _cancel(client, "MIX-1").status_code == 200
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 100.0
    assert client.get("/batches/MIX-1").json()["remaining_qty"] == 0.0
    assert client.get("/batches/FG-1").json()["remaining_qty"] == 0.0
    # 返还后的原料可再次用于新的生产（编号空间不含已撤销产出以外的复用限制）
    r = _produce(client, "MIX-3", [("RM-A", "20")], key="3")
    assert r.status_code == 201, r.text
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 80.0


# ------------------------------------------------------------- 拒绝条件


def test_cancel_nonexistent_batch_404(client):
    _raw(client, "RM-A", "100")
    r = _cancel(client, "GHOST")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "SOURCE_NOT_FOUND"
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 100.0


def test_cancel_raw_batch_rejected(client):
    _raw(client, "RM-A", "100")
    r = _cancel(client, "RM-A")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "NOT_PRODUCTION_OUTPUT"
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 100.0


def test_cancel_suspect_output_rejected(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    # 直接把产出标记为疑似
    client.post(
        "/contamination", json={"batch_no": "MIX-1"}, headers={"Idempotency-Key": "c"}
    )
    r = _cancel(client, "MIX-1")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "BATCH_SUSPECT"
    # 库存不变、未撤销
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 60.0
    assert client.get("/batches/MIX-1").json()["is_cancelled"] is False

    # 经复核撤销疑似后（产出此时有复核记录），撤销仍应被拒绝：有复核记录
    rv = client.post(
        "/reviews",
        json={"batch_no": "MIX-1", "review_note": "误报"},
        headers={"Idempotency-Key": "r"},
    )
    assert rv.status_code == 200
    r = _cancel(client, "MIX-1", key="cancel-2")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "PRODUCTION_HAS_REVIEW"
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 60.0


def test_cancel_output_with_review_record_rejected(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    # 上游疑似导致 MIX-1 隔离，复核上游后给 MIX-1 自身留复核记录的路径：
    # 直接对 MIX-1 标记再复核，形成复核记录
    client.post(
        "/contamination", json={"batch_no": "MIX-1"}, headers={"Idempotency-Key": "c"}
    )
    client.post(
        "/reviews",
        json={"batch_no": "MIX-1", "review_note": "复检合格"},
        headers={"Idempotency-Key": "r"},
    )
    r = _cancel(client, "MIX-1")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "PRODUCTION_HAS_REVIEW"


def test_cancel_twice_rejected(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    assert _cancel(client, "MIX-1", key="c1").status_code == 200
    r = _cancel(client, "MIX-1", key="c2")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "PRODUCTION_ALREADY_CANCELLED"
    # 重复撤销不再次返还
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 100.0


def test_cancel_requires_nonblank_reason(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    r = client.post(
        "/productions/MIX-1/cancellation",
        json={"reason": "   "},
        headers={"Idempotency-Key": "c1"},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "VALIDATION_ERROR"
    r = client.post(
        "/productions/MIX-1/cancellation",
        json={},
        headers={"Idempotency-Key": "c2"},
    )
    assert r.status_code == 422
    assert client.get("/batches/MIX-1").json()["is_cancelled"] is False
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 60.0


def test_cancel_requires_idempotency_key(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    r = client.post("/productions/MIX-1/cancellation", json={"reason": "x"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REQUIRED"
    assert client.get("/batches/MIX-1").json()["is_cancelled"] is False


# ------------------------------------------------------------- 查询原单据


def test_get_production_detail_before_and_after_cancel(client):
    _raw(client, "RM-A", "100")
    _raw(client, "RM-B", "100")
    _produce(client, "MIX-1", [("RM-A", "40"), ("RM-B", "20")], out_qty=50, key="1")

    r = client.get("/productions/MIX-1")
    assert r.status_code == 200, r.text
    assert r.json() == {
        "output_batch_no": "MIX-1",
        "unit": "kg",
        "output_quantity": 50.0,
        "is_cancelled": False,
        "inputs": [
            {"batch_no": "RM-A", "quantity": 40.0},
            {"batch_no": "RM-B", "quantity": 20.0},
        ],
        "cancellation": None,
    }

    cr = _cancel(client, "MIX-1", "操作员录入错误")
    cancelled_at = cr.json()["cancelled_at"]
    r = client.get("/productions/MIX-1")
    assert r.status_code == 200
    body = r.json()
    assert body["is_cancelled"] is True
    # 原单据保留
    assert body["output_quantity"] == 50.0
    assert body["inputs"] == [
        {"batch_no": "RM-A", "quantity": 40.0},
        {"batch_no": "RM-B", "quantity": 20.0},
    ]
    assert body["cancellation"] == {
        "reason": "操作员录入错误",
        "cancelled_at": cancelled_at,
    }


def test_get_production_detail_errors(client):
    _raw(client, "RM-A", "100")
    assert client.get("/productions/GHOST").status_code == 404
    r = client.get("/productions/RM-A")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "NOT_PRODUCTION_OUTPUT"


# ------------------------------------------------------------- 幂等


def test_cancel_idempotent_replay(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    payload = {"reason": "录错"}
    r1 = client.post(
        "/productions/MIX-1/cancellation",
        json=payload,
        headers={"Idempotency-Key": "K1"},
    )
    assert r1.status_code == 200
    # 同键同体重放：返回首次结果，不重复返还
    r2 = client.post(
        "/productions/MIX-1/cancellation",
        json=payload,
        headers={"Idempotency-Key": "K1"},
    )
    assert r2.status_code == 200
    assert r2.json() == r1.json()
    assert client.get("/batches/RM-A").json()["remaining_qty"] == 100.0


def test_cancel_same_key_different_body_conflicts(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    assert client.post(
        "/productions/MIX-1/cancellation",
        json={"reason": "原因一"},
        headers={"Idempotency-Key": "K"},
    ).status_code == 200
    r = client.post(
        "/productions/MIX-1/cancellation",
        json={"reason": "原因二"},
        headers={"Idempotency-Key": "K"},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_failed_cancel_not_stored_same_key_retryable(client):
    _raw(client, "RM-A", "100")
    _produce(client, "MIX-1", [("RM-A", "40")], key="1")
    # 先用该键打一个失败请求（目标尚不存在的另一单据）——失败不固化
    r1 = client.post(
        "/productions/MIX-9/cancellation",
        json={"reason": "x"},
        headers={"Idempotency-Key": "K"},
    )
    assert r1.status_code == 404
    # 同键对 MIX-1 撤销：路径不同，键空间独立，正常成功
    r2 = client.post(
        "/productions/MIX-1/cancellation",
        json={"reason": "x"},
        headers={"Idempotency-Key": "K"},
    )
    assert r2.status_code == 200, r2.text


# ------------------------------------------------------------- 并发


def test_concurrent_cancel_and_produce_equivalent_to_serial(engine):
    """撤销与投料并发：投料要么先于撤销成功（撤销随后被拒：有下游），
    要么撤销先成功（投料随后被拒：产出已撤销）。两种结局都等价于某一串行顺序，
    库存不能超用、撤销不能误放行。"""
    with session_scope() as s:
        service.register_raw_batch(s, RawBatchIn(batch_no="RM-1", unit="kg", quantity="100"))
        service.submit_production(
            s,
            ProductionIn(
                output_batch_no="MIX-1",
                unit="kg",
                output_quantity=Decimal("40"),
                inputs=[{"batch_no": "RM-1", "quantity": Decimal("40")}],  # type: ignore[list-item]
            ),
        )

    barrier = threading.Barrier(2)
    results: dict[str, str] = {}

    def cancel_worker() -> None:
        barrier.wait()
        try:
            with session_scope() as s:
                service.cancel_production(
                    s, "MIX-1", CancelProductionIn(reason="录错撤销")
                )
            results["cancel"] = "ok"
        except AppError as exc:
            results["cancel"] = exc.code
        except Exception as exc:
            results["cancel"] = f"ERROR:{type(exc).__name__}:{exc}"

    def produce_worker() -> None:
        barrier.wait()
        try:
            with session_scope() as s:
                service.submit_production(
                    s,
                    ProductionIn(
                        output_batch_no="FG-1",
                        unit="kg",
                        output_quantity=Decimal("10"),
                        inputs=[{"batch_no": "MIX-1", "quantity": Decimal("10")}],  # type: ignore[list-item]
                    ),
                )
            results["produce"] = "ok"
        except AppError as exc:
            results["produce"] = exc.code
        except Exception as exc:
            results["produce"] = f"ERROR:{type(exc).__name__}:{exc}"

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda f: f(), [cancel_worker, produce_worker]))

    assert not any(str(v).startswith("ERROR") for v in results.values()), results
    assert {results["cancel"], results["produce"]} in (
        {"ok", "PRODUCTION_HAS_DOWNSTREAM"},
        {"ok", "BATCH_CANCELLED"},
    ), results

    with session_scope() as s:
        rm1 = s.get(Batch, "RM-1")
        mix1 = s.get(Batch, "MIX-1")
        fg1 = s.get(Batch, "FG-1")
        prod = s.execute(
            Production.__table__.select().where(Production.output_no == "MIX-1")
        ).mappings().one()
        if results["cancel"] == "ok" and results["produce"] == "BATCH_CANCELLED":
            # 撤销先：RM-1 返还到 100；MIX-1 作废；FG-1 不存在
            assert rm1.remaining_qty == Decimal("100.0000")
            assert mix1.remaining_qty == 0
            assert prod["is_cancelled"] is True
            assert fg1 is None
        else:
            # 投料先：FG-1 存在并消耗 10，撤销被拒，库存不返还
            assert results["produce"] == "ok"
            assert results["cancel"] == "PRODUCTION_HAS_DOWNSTREAM"
            assert rm1.remaining_qty == Decimal("60.0000")
            assert mix1.remaining_qty == Decimal("30.0000")
            assert fg1 is not None
            assert prod["is_cancelled"] is False


def test_concurrent_double_cancel_only_one_wins(engine):
    """两个请求并发撤销同一张单：恰好一个成功，另一个明确拒绝且不二次返还。"""
    with session_scope() as s:
        service.register_raw_batch(s, RawBatchIn(batch_no="RM-1", unit="kg", quantity="100"))
        service.submit_production(
            s,
            ProductionIn(
                output_batch_no="MIX-1",
                unit="kg",
                output_quantity=Decimal("40"),
                inputs=[{"batch_no": "RM-1", "quantity": Decimal("40")}],  # type: ignore[list-item]
            ),
        )

    barrier = threading.Barrier(2)
    results: list[str] = []

    def worker() -> None:
        barrier.wait()
        try:
            with session_scope() as s:
                service.cancel_production(
                    s, "MIX-1", CancelProductionIn(reason="录错撤销")
                )
            results.append("ok")
        except AppError as exc:
            results.append(exc.code)
        except Exception as exc:
            results.append(f"ERROR:{type(exc).__name__}:{exc}")

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: worker(), range(2)))

    assert results.count("ok") == 1, results
    assert results.count("PRODUCTION_ALREADY_CANCELLED") == 1, results
    with session_scope() as s:
        assert s.get(Batch, "RM-1").remaining_qty == Decimal("100.0000")
        assert s.get(Batch, "MIX-1").remaining_qty == 0


def test_concurrent_cancel_and_flag_equivalent_to_serial(engine):
    """撤销与污染标记并发：等价于（撤销→标记）或（标记→撤销拒绝）两种串行结局。"""
    with session_scope() as s:
        service.register_raw_batch(s, RawBatchIn(batch_no="RM-1", unit="kg", quantity="100"))
        service.submit_production(
            s,
            ProductionIn(
                output_batch_no="MIX-1",
                unit="kg",
                output_quantity=Decimal("40"),
                inputs=[{"batch_no": "RM-1", "quantity": Decimal("40")}],  # type: ignore[list-item]
            ),
        )

    barrier = threading.Barrier(2)
    results: dict[str, str] = {}

    def cancel_worker() -> None:
        barrier.wait()
        try:
            with session_scope() as s:
                service.cancel_production(
                    s, "MIX-1", CancelProductionIn(reason="录错")
                )
            results["cancel"] = "ok"
        except AppError as exc:
            results["cancel"] = exc.code
        except Exception as exc:
            results["cancel"] = f"ERROR:{type(exc).__name__}:{exc}"

    def flag_worker() -> None:
        barrier.wait()
        try:
            with session_scope() as s:
                # 标记 MIX-1 自身疑似（而非 RM-1，避免与撤销返还路径无关的传播）
                service.mark_suspect(s, SuspectIn(batch_no="MIX-1"))
            results["flag"] = "ok"
        except Exception as exc:
            results["flag"] = f"ERROR:{type(exc).__name__}:{exc}"

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda f: f(), [cancel_worker, flag_worker]))

    assert results["flag"] == "ok", results
    assert not any(str(v).startswith("ERROR") for v in results.values()), results
    assert results["cancel"] in ("ok", "BATCH_SUSPECT"), results

    with session_scope() as s:
        mix1 = s.get(Batch, "MIX-1")
        prod = s.execute(
            Production.__table__.select().where(Production.output_no == "MIX-1")
        ).mappings().one()
        if results["cancel"] == "ok":
            # 撤销先：标记后 MIX-1 虽疑似，但单据已撤销、边不传播；RM-1 已返还
            assert prod["is_cancelled"] is True
            assert s.get(Batch, "RM-1").remaining_qty == Decimal("100.0000")
            assert mix1.is_suspect is True
        else:
            # 标记先：撤销拒绝，库存不变
            assert prod["is_cancelled"] is False
            assert s.get(Batch, "RM-1").remaining_qty == Decimal("60.0000")
