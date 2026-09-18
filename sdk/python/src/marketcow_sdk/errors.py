from __future__ import annotations

from typing import Any, Mapping


class MarketCowError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str = "marketcow_error",
        status_code: int | None = None,
        request_id: str | None = None,
        provider: str | None = None,
        instrument_id: str | None = None,
        retry_after: float | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.request_id = request_id
        self.provider = provider
        self.instrument_id = instrument_id
        self.retry_after = retry_after
        self.details = dict(details or {})


class TransportError(MarketCowError):
    pass


class MarketCowTimeoutError(TransportError):
    pass


class AuthenticationError(MarketCowError):
    pass


class PermissionDeniedError(MarketCowError):
    pass


class RateLimitError(MarketCowError):
    pass


class RequestValidationError(MarketCowError):
    pass


class InstrumentNotFoundError(MarketCowError):
    pass


class InstrumentAmbiguousError(MarketCowError):
    pass


class ProviderNotSupportedError(MarketCowError):
    pass


class ProviderUnavailableError(MarketCowError):
    pass


class DataUnavailableError(MarketCowError):
    pass


class StaleDataError(MarketCowError):
    pass


class SnapshotChangedError(MarketCowError):
    pass


class SchemaVersionError(MarketCowError):
    pass


_CODE_ERRORS: dict[str, type[MarketCowError]] = {
    "instrument_not_found": InstrumentNotFoundError,
    "not_found": InstrumentNotFoundError,
    "ambiguous": InstrumentAmbiguousError,
    "provider_not_supported": ProviderNotSupportedError,
    "provider_unavailable": ProviderUnavailableError,
    "no_data": DataUnavailableError,
    "stale_data": StaleDataError,
    "canonical_snapshot_changed": SnapshotChangedError,
    "rate_limit_exceeded": RateLimitError,
}


def error_from_response(
    status_code: int,
    payload: Any,
    *,
    request_id: str | None,
    retry_after: float | None = None,
) -> MarketCowError:
    detail = payload.get("detail", payload) if isinstance(payload, dict) else {}
    if not isinstance(detail, dict):
        detail = {"message": str(detail)}
    code = str(detail.get("code") or f"http_{status_code}")
    message = str(detail.get("message") or detail.get("error") or code)
    error_type = _CODE_ERRORS.get(code)
    if error_type is None:
        if status_code == 401:
            error_type = AuthenticationError
        elif status_code == 403:
            error_type = PermissionDeniedError
        elif status_code == 429:
            error_type = RateLimitError
        elif status_code in {400, 409, 422}:
            error_type = RequestValidationError
        elif status_code in {502, 503, 504}:
            error_type = ProviderUnavailableError
        else:
            error_type = MarketCowError
    return error_type(
        message,
        code=code,
        status_code=status_code,
        request_id=request_id,
        provider=detail.get("provider"),
        instrument_id=detail.get("instrument_id") or detail.get("symbol"),
        retry_after=retry_after,
        details=detail,
    )
