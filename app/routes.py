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
    DescendantsOut,
    LineageOut,
    ProductionIn,
    ProductionOut,
    RawBatchIn,
    ReviewIn,
    ReviewOut,
    SuspectIn,
)

router = APIRouter()


def _batch_out(batch: Batch) -> dict[str, Any]:
    return {
        "batch_no": batch.batch_no,
        "unit": batch.unit,
        "initial_qty": float(batch.initial_qty),
        "remaining_qty": float(batch.remaining_qty),
        "is_suspect": batch.is_suspect,
        "is_quarantined": batch.is_quarantined,
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
        return status.HTTP_201_CREATED, _batch_out(batch)

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


@router.get("/batches/{batch_no}", response_model=BatchOut)
def get_batch(batch_no: str, session: SessionDep) -> dict[str, Any]:
    batch = session.get(Batch, batch_no)
    if batch is None:
        from .errors import batch_not_found

        raise batch_not_found(batch_no)
    return _batch_out(batch)


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
