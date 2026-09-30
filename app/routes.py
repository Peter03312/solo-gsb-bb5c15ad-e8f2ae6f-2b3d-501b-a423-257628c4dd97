"""批次追溯 HTTP 路由。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, Request, Response, status
from sqlalchemy.orm import Session

from . import service
from .deps import SessionDep
from .errors import AppError
from .models import Batch, Production
from .schemas import (
    BatchOut,
    CancelProductionIn,
    CancellationOut,
    DescendantsOut,
    LineageOut,
    ProductionDetailOut,
    ProductionIn,
    ProductionOut,
    RawBatchIn,
    ReviewIn,
    ReviewOut,
    SuspectIn,
)

router = APIRouter()


def _batch_out(session: Session, batch: Batch) -> dict[str, Any]:
    return {
        "batch_no": batch.batch_no,
        "unit": batch.unit,
        "initial_qty": float(batch.initial_qty),
        "remaining_qty": float(batch.remaining_qty),
        "is_suspect": batch.is_suspect,
        "is_quarantined": batch.is_quarantined,
        "is_cancelled": service.is_cancelled_batch(session, batch.batch_no),
        "note": batch.note,
    }


def _production_out(prod: Production, unit: str) -> dict[str, Any]:
    return {
        "output_batch_no": prod.output_no,
        "unit": unit,
        "output_quantity": float(prod.output_qty),
        "inputs": [
            {"batch_no": pi.source_no, "quantity": float(pi.qty)}
            for pi in sorted(prod.inputs, key=lambda x: x.id)
        ],
    }


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/raw-batches", response_model=BatchOut, status_code=status.HTTP_201_CREATED)
def create_raw_batch(
    body: RawBatchIn,
    session: SessionDep,
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    if not idempotency_key:
        raise AppError(
            "IDEMPOTENCY_KEY_REQUIRED",
            "写操作必须携带 Idempotency-Key 请求头以支持安全重试",
            400,
        )

    payload = body.model_dump(mode="json")

    def action() -> tuple[int, dict[str, Any]]:
        batch = service.register_raw_batch(session, body)
        return status.HTTP_201_CREATED, _batch_out(session, batch)

    code, result = service.idempotent(
        session,
        method=request.method,
        path=request.url.path,
        key=idempotency_key,
        payload=payload,
        action=action,
    )
    response.status_code = code
    return result


@router.post("/productions", response_model=ProductionOut, status_code=status.HTTP_201_CREATED)
def create_production(
    body: ProductionIn,
    session: SessionDep,
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    if not idempotency_key:
        raise AppError(
            "IDEMPOTENCY_KEY_REQUIRED",
            "写操作必须携带 Idempotency-Key 请求头以支持安全重试",
            400,
        )

    payload = body.model_dump(mode="json")

    def action() -> tuple[int, dict[str, Any]]:
        prod = service.submit_production(session, body)
        return status.HTTP_201_CREATED, _production_out(prod, body.unit)

    code, result = service.idempotent(
        session,
        method=request.method,
        path=request.url.path,
        key=idempotency_key,
        payload=payload,
        action=action,
    )
    response.status_code = code
    return result


def _production_detail_out(prod: Production, unit: str) -> dict[str, Any]:
    return {
        "output_batch_no": prod.output_no,
        "unit": unit,
        "output_quantity": float(prod.output_qty),
        "is_cancelled": prod.is_cancelled,
        "inputs": [
            {"batch_no": pi.source_no, "quantity": float(pi.qty)}
            for pi in sorted(prod.inputs, key=lambda x: x.id)
        ],
        "cancellation": (
            {
                "reason": prod.cancellation.reason,
                "cancelled_at": prod.cancellation.cancelled_at,
            }
            if prod.cancellation is not None
            else None
        ),
    }


def _cancellation_out(result: service.CancelResult) -> dict[str, Any]:
    return {
        "output_batch_no": result.output.batch_no,
        "status": "cancelled",
        "reason": result.reason,
        "cancelled_at": result.cancelled_at,
        "returns": [
            {
                "source_batch_no": source.batch_no,
                "returned_quantity": float(qty),
                "remaining_after": float(source.remaining_qty),
            }
            for source, qty in sorted(
                result.returns, key=lambda item: item[0].batch_no
            )
        ],
    }


@router.get("/productions/{output_no}", response_model=ProductionDetailOut)
def get_production(output_no: str, session: SessionDep) -> dict[str, Any]:
    """按产出批次编号查询生产原单据及撤销状态（撤销后原单据保留可查）。"""
    prod, output = service.get_production_detail(session, output_no)
    return _production_detail_out(prod, output.unit)


@router.post(
    "/productions/{output_no}/cancellation",
    response_model=CancellationOut,
    status_code=status.HTTP_200_OK,
)
def cancel_production(
    output_no: str,
    body: CancelProductionIn,
    session: SessionDep,
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    """撤销生产单：返还各来源原投料量，撤销的投料边退出有效谱系。必须携带幂等键。"""
    if not idempotency_key:
        raise AppError(
            "IDEMPOTENCY_KEY_REQUIRED",
            "写操作必须携带 Idempotency-Key 请求头以支持安全重试",
            400,
        )

    payload = {"output_batch_no": output_no, **body.model_dump(mode="json")}

    def action() -> tuple[int, dict[str, Any]]:
        result = service.cancel_production(session, output_no, body)
        return status.HTTP_200_OK, _cancellation_out(result)

    code, result = service.idempotent(
        session,
        method=request.method,
        path=request.url.path,
        key=idempotency_key,
        payload=payload,
        action=action,
    )
    response.status_code = code
    return result


@router.get("/batches/{batch_no}", response_model=BatchOut)
def get_batch(batch_no: str, session: SessionDep) -> dict[str, Any]:
    batch = session.get(Batch, batch_no)
    if batch is None:
        from .errors import batch_not_found

        raise batch_not_found(batch_no)
    return _batch_out(session, batch)


@router.get("/batches/{batch_no}/lineage", response_model=LineageOut)
def get_lineage(batch_no: str, session: SessionDep) -> dict[str, Any]:
    return service.lineage(session, batch_no)


@router.post("/contamination", response_model=DescendantsOut, status_code=status.HTTP_200_OK)
def flag_suspect(
    body: SuspectIn,
    session: SessionDep,
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    # 污染标记本身是幂等动作：无键也允许，有键则走标准重放/冲突语义
    payload = body.model_dump(mode="json")

    def action() -> tuple[int, dict[str, Any]]:
        return status.HTTP_200_OK, service.mark_suspect(session, body)

    code, result = service.idempotent(
        session,
        method=request.method,
        path=request.url.path,
        key=idempotency_key,
        payload=payload,
        action=action,
    )
    response.status_code = code
    return result


@router.post("/reviews", response_model=ReviewOut, status_code=status.HTTP_200_OK)
def review_suspect(
    body: ReviewIn,
    session: SessionDep,
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    """质检复核：确认疑似批次为误报，撤销标记并重算隔离。必须携带幂等键。"""
    if not idempotency_key:
        raise AppError(
            "IDEMPOTENCY_KEY_REQUIRED",
            "写操作必须携带 Idempotency-Key 请求头以支持安全重试",
            400,
        )

    payload = body.model_dump(mode="json")

    def action() -> tuple[int, dict[str, Any]]:
        return status.HTTP_200_OK, service.review_suspect(session, body)

    code, result = service.idempotent(
        session,
        method=request.method,
        path=request.url.path,
        key=idempotency_key,
        payload=payload,
        action=action,
    )
    response.status_code = code
    return result
