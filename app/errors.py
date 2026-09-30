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
