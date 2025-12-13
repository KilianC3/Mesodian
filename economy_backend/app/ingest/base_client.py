"""
Shared HTTP client infrastructure for ingestion pipelines. Implements retry
policies, backoff, and resilience helpers that provider-specific clients reuse
to fetch economic, trade, finance, and ESG data reliably.
"""

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx


logger = logging.getLogger(__name__)


class HttpClientError(Exception):
    """Raised when an HTTP request fails after retries."""


class SDMXClientError(Exception):
    """Raised when an SDMX request or parsing fails."""


@dataclass
class ProviderLimits:
    max_retries: int
    backoff_base_seconds: float
    timeout_seconds: float


PROVIDER_LIMITS: Dict[str, ProviderLimits] = {
    "FRED": ProviderLimits(max_retries=3, backoff_base_seconds=0.5, timeout_seconds=10.0),
    "WDI": ProviderLimits(max_retries=4, backoff_base_seconds=0.75, timeout_seconds=15.0),
    "COMTRADE": ProviderLimits(max_retries=5, backoff_base_seconds=1.0, timeout_seconds=30.0),
    "PATENTSVIEW": ProviderLimits(max_retries=3, backoff_base_seconds=1.0, timeout_seconds=20.0),
    "EIA": ProviderLimits(max_retries=4, backoff_base_seconds=0.5, timeout_seconds=20.0),
    "EMBER": ProviderLimits(max_retries=3, backoff_base_seconds=1.0, timeout_seconds=30.0),
    "YFINANCE": ProviderLimits(max_retries=2, backoff_base_seconds=0.5, timeout_seconds=10.0),
    "STOOQ": ProviderLimits(max_retries=2, backoff_base_seconds=0.5, timeout_seconds=10.0),
    "FAOSTAT": ProviderLimits(max_retries=4, backoff_base_seconds=1.5, timeout_seconds=60.0),
    "AFDB": ProviderLimits(max_retries=4, backoff_base_seconds=1.5, timeout_seconds=60.0),
    "OECD": ProviderLimits(max_retries=4, backoff_base_seconds=1.0, timeout_seconds=30.0),
    "EUROSTAT": ProviderLimits(max_retries=3, backoff_base_seconds=1.0, timeout_seconds=30.0),
    "IMF": ProviderLimits(max_retries=4, backoff_base_seconds=1.0, timeout_seconds=30.0),
    "ECB": ProviderLimits(max_retries=3, backoff_base_seconds=0.75, timeout_seconds=20.0),
    "BIS": ProviderLimits(max_retries=4, backoff_base_seconds=1.0, timeout_seconds=30.0),
    "UNCTAD": ProviderLimits(max_retries=4, backoff_base_seconds=1.5, timeout_seconds=90.0),
    "GDELT": ProviderLimits(max_retries=3, backoff_base_seconds=1.0, timeout_seconds=60.0),
    "OPENALEX": ProviderLimits(max_retries=3, backoff_base_seconds=1.0, timeout_seconds=30.0),
}


class AsyncHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 10.0,
        max_retries: int = 3,
        backoff_base_seconds: float = 0.5,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self._client: Optional[httpx.AsyncClient] = client
        self._owns_client = client is None

    async def __aenter__(self) -> "AsyncHttpClient":
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout_seconds)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client and self._owns_client:
            await self._client.aclose()
        self._client = None

    async def _sleep_with_backoff(self, attempt: int) -> None:
        delay = self.backoff_base_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0, self.backoff_base_seconds)
        delay += jitter
        logger.debug("Retrying after %.2f seconds (attempt %s)", delay, attempt)
        await asyncio.sleep(delay)

    async def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout_seconds)

        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt <= self.max_retries:
            attempt += 1
            try:
                logger.info("GET %s params=%s attempt=%s", path, params, attempt)
                response = await self._client.get(path, params=params)
                logger.info("Response %s for %s", response.status_code, response.url)
            except httpx.HTTPError as exc:
                last_exc = exc
                logger.warning("HTTP error on attempt %s for %s: %s", attempt, path, exc)
                if attempt > self.max_retries:
                    raise HttpClientError(f"Request to {path} failed after retries") from exc
                await self._sleep_with_backoff(attempt)
                continue

            if response.status_code in {429} or 500 <= response.status_code < 600:
                if attempt > self.max_retries:
                    self._raise_for_status(response)
                logger.warning(
                    "Retryable response %s on attempt %s for %s", response.status_code, attempt, path
                )
                await self._sleep_with_backoff(attempt)
                continue

            if response.is_error:
                self._raise_for_status(response)

            return response

        if last_exc:
            raise HttpClientError(f"Request to {path} failed after retries") from last_exc
        raise HttpClientError(f"Request to {path} failed after retries")

    def _raise_for_status(self, response: httpx.Response) -> None:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            request = response.request
            message = (
                f"Request to {request.url if request else 'unknown'} failed with status {response.status_code}"
            )
            raise HttpClientError(message) from exc

    async def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        response = await self._request(path, params=params)
        try:
            return response.json()
        except Exception as exc:  # pragma: no cover - httpx defines JSON-related errors
            raise HttpClientError(f"Failed to decode JSON response from {response.url}") from exc

    async def get_text(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        response = await self._request(path, params=params)
        return response.text


def get_provider_client(provider_name: str, base_url: str) -> AsyncHttpClient:
    limits = PROVIDER_LIMITS.get(provider_name.upper())
    if limits is None:
        raise ValueError(f"Unknown provider: {provider_name}")
    return AsyncHttpClient(
        base_url=base_url,
        timeout_seconds=limits.timeout_seconds,
        max_retries=limits.max_retries,
        backoff_base_seconds=limits.backoff_base_seconds,
    )


def fetch_sdmx_dataset(
    base_url: str,
    dataset_code: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    sample_config: Optional["SampleConfig"] = None,
):
    """Fetch SDMX dataset with optional sample mode limiting."""
    try:
        import pandas as pd
        import pandasdmx
    except ImportError as exc:  # pragma: no cover - depends on optional dependency
        raise SDMXClientError("pandas and pandasdmx are required to fetch SDMX datasets") from exc
    
    # Import SampleConfig if needed for type checking
    if sample_config is None:
        from app.ingest.sample_mode import SampleConfig
        sample_config = SampleConfig()

    params = params or {}
    
    # Add limiting parameters for SDMX in sample mode
    if sample_config.enabled:
        # Standard SDMX parameters for limiting
        params["firstNObservations"] = str(sample_config.max_records_per_country)
    
    url = f"{base_url.rstrip('/')}/data/{dataset_code}"

    try:
        response = httpx.get(url, params=params, timeout=30.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise SDMXClientError(f"SDMX request to {url} failed: {exc}") from exc

    try:
        # pandasdmx needs a file-like object, not bytes
        from io import BytesIO
        message = pandasdmx.read_sdmx(BytesIO(response.content))
        if not message.data:
            raise SDMXClientError(f"SDMX response from {url} contained no data")
        dataset = message.data[0]
        # Use pandasdmx.to_pandas() for GenericDataSet objects
        data = pandasdmx.to_pandas(dataset)
        if isinstance(data, pd.Series):
            df = data.rename("value").reset_index()
        else:
            df = data.reset_index()
        df = df.rename(columns={"TIME_PERIOD": "time"})
        if "value" not in df.columns:
            value_columns = [col for col in df.columns if str(col).lower() in {"obs_value", "value"}]
            if value_columns:
                df = df.rename(columns={value_columns[0]: "value"})
        
        # Additional limiting in case firstNObservations wasn't respected
        if sample_config.enabled and len(df) > sample_config.max_records_per_country:
            df = df.tail(sample_config.max_records_per_country)
        
        return df
    except Exception as exc:  # pragma: no cover - parsing errors are data dependent
        raise SDMXClientError(f"Failed to parse SDMX response from {url}: {exc}") from exc
