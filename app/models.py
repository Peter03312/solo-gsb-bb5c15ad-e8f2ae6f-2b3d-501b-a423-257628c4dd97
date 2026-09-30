"""ORM 模型：批次、生产单、投料明细、幂等记录。"""
from __future__ import annotations

from decimal import Decimal

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    false,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base

# Numeric 精度：整数 18 位、小数 4 位，满足食品厂计量场景
QTY = Numeric(18, 4)


class Batch(Base):
    """批次：既可能是原料批次，也可能是生产产出批次。"""

    __tablename__ = "batches"

    batch_no: Mapped[str] = mapped_column(String(64), primary_key=True)
    unit: Mapped[str] = mapped_column(String(16), nullable=False)
    initial_qty: Mapped[Decimal] = mapped_column(QTY, nullable=False)
    remaining_qty: Mapped[Decimal] = mapped_column(QTY, nullable=False)
    # 被人工/检测标记为疑似污染的批次
    is_suspect: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=false())
    # 隔离中：自身疑似，或谱系上有疑似祖先；隔离批次禁止再投料
    is_quarantined: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=false())
    note: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    inputs: Mapped[list["ProductionInput"]] = relationship(
        back_populates="source_batch", foreign_keys="ProductionInput.source_no"
    )

    __table_args__ = (
        CheckConstraint("initial_qty > 0", name="ck_batch_initial_qty_positive"),
        CheckConstraint("remaining_qty >= 0", name="ck_batch_remaining_non_negative"),
        CheckConstraint("remaining_qty <= initial_qty", name="ck_batch_remaining_within"),
    )


class Production(Base):
    """一次生产：多笔投入 -> 一个产出批次。"""

    __tablename__ = "productions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    output_no: Mapped[str] = mapped_column(
        String(64), ForeignKey("batches.batch_no"), nullable=False, unique=True
    )
    output_qty: Mapped[Decimal] = mapped_column(QTY, nullable=False)
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    inputs: Mapped[list["ProductionInput"]] = relationship(
        back_populates="production", cascade="all, delete-orphan"
    )
    output: Mapped[Batch] = relationship(foreign_keys=[output_no])

    __table_args__ = (
        CheckConstraint("output_qty > 0", name="ck_production_output_positive"),
    )


class ProductionInput(Base):
    """生产单中的一笔原料消耗。"""

    __tablename__ = "production_inputs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    production_id: Mapped[int] = mapped_column(
        ForeignKey("productions.id", ondelete="CASCADE"), nullable=False
    )
    source_no: Mapped[str] = mapped_column(
        String(64), ForeignKey("batches.batch_no"), nullable=False
    )
    qty: Mapped[Decimal] = mapped_column(QTY, nullable=False)

    production: Mapped[Production] = relationship(back_populates="inputs")
    source_batch: Mapped[Batch] = relationship(
        back_populates="inputs", foreign_keys=[source_no]
    )

    __table_args__ = (
        UniqueConstraint("production_id", "source_no", name="uq_input_prod_source"),
        CheckConstraint("qty > 0", name="ck_production_input_qty_positive"),
        Index("ix_prod_input_source", "source_no"),
    )


class BatchReview(Base):
    """质检复核记录：确认某疑似批次为误报后撤销其疑似标记。

    每个批次至多一条有效复核；被复核批次必须当时处于疑似状态。
    复核只撤销“自身疑似”，隔离状态随后按谱系重算（混料来源仍疑似的批次继续隔离）。
    """

    __tablename__ = "batch_reviews"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    batch_no: Mapped[str] = mapped_column(
        String(64), ForeignKey("batches.batch_no"), nullable=False, unique=True
    )
    review_note: Mapped[str] = mapped_column(Text, nullable=False)
    reviewed_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    batch: Mapped[Batch] = relationship(foreign_keys=[batch_no])


class IdempotencyKey(Base):
    """请求幂等记录。

    同一 (method, path, key) 只允许一种请求体（sha256）；
    成功后固化状态码与响应体，重试原样返回，异内容报冲突。
    """

    __tablename__ = "idempotency_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    method: Mapped[str] = mapped_column(String(8), nullable=False)
    path: Mapped[str] = mapped_column(String(128), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response_body: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("method", "path", "key", name="uq_idem_endpoint_key"),
    )
