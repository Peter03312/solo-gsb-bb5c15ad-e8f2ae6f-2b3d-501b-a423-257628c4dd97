"""统一业务错误：携带稳定错误码，映射为 4xx 响应。"""
from __future__ import annotations


class AppError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 409, details=None):  # noqa: ANN001
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details


def batch_exists(batch_no: str) -> AppError:
    return AppError(
        "BATCH_NO_REUSED",
        f"批次编号 {batch_no!r} 已存在，批次编号不得复用",
        409,
    )


def batch_not_found(batch_no: str) -> AppError:
    return AppError(
        "SOURCE_NOT_FOUND",
        f"批次 {batch_no!r} 不存在",
        404,
    )


def batch_quarantined(batch_no: str) -> AppError:
    return AppError(
        "BATCH_QUARANTINED",
        f"批次 {batch_no!r} 已隔离，隔离批次不得再投料",
        409,
    )


def batch_not_suspect(batch_no: str) -> AppError:
    return AppError(
        "BATCH_NOT_SUSPECT",
        f"批次 {batch_no!r} 当前不是疑似批次（编号不存在或已复核撤销），无可撤销的疑似标记",
        409,
        details={"batch_no": batch_no},
    )


def batch_suspect(batch_no: str) -> AppError:
    return AppError(
        "BATCH_SUSPECT",
        f"批次 {batch_no!r} 当前为疑似污染批次，不得撤销生产单",
        409,
        details={"batch_no": batch_no},
    )


def insufficient(batch_no: str, requested: object, remaining: object) -> AppError:
    return AppError(
        "INSUFFICIENT_BALANCE",
        f"批次 {batch_no!r} 余量不足：请求 {requested}，剩余 {remaining}",
        409,
        details={"batch_no": batch_no, "requested": str(requested), "remaining": str(remaining)},
    )


def unit_mismatch(batch_no: str, expected: str, actual: str) -> AppError:
    return AppError(
        "UNIT_MISMATCH",
        f"批次 {batch_no!r} 单位为 {expected!r}，与申报单位 {actual!r} 不一致",
        422,
    )


def batch_not_production(batch_no: str) -> AppError:
    return AppError(
        "NOT_PRODUCTION_OUTPUT",
        f"批次 {batch_no!r} 不是生产产出批次（无生产单），不能撤销生产单",
        409,
        details={"batch_no": batch_no},
    )


def production_has_downstream(batch_no: str, consumers: list[str]) -> AppError:
    return AppError(
        "PRODUCTION_HAS_DOWNSTREAM",
        f"产出批次 {batch_no!r} 已被后续生产投料（{', '.join(consumers)}），不得撤销",
        409,
        details={"batch_no": batch_no, "consumers": consumers},
    )


def production_has_review(batch_no: str) -> AppError:
    return AppError(
        "PRODUCTION_HAS_REVIEW",
        f"产出批次 {batch_no!r} 存在质检复核记录，生产单不得撤销",
        409,
        details={"batch_no": batch_no},
    )


def production_already_cancelled(batch_no: str) -> AppError:
    return AppError(
        "PRODUCTION_ALREADY_CANCELLED",
        f"产出批次 {batch_no!r} 的生产单已撤销，不得重复撤销",
        409,
        details={"batch_no": batch_no},
    )


def output_cancelled(batch_no: str) -> AppError:
    return AppError(
        "BATCH_CANCELLED",
        f"批次 {batch_no!r} 是已撤销生产单的产出，不得再投料",
        409,
        details={"batch_no": batch_no},
    )
