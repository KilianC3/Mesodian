"""Lightweight DB.nomics client for discovery and data access."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import time

import httpx
import pandas as pd

DEFAULT_BASE_URL = "https://api.worldbank.org/v2/"  # placeholder overridden by config


@dataclass
class ProviderInfo:
    code: str
    name: str
    description: Optional[str]


@dataclass
class DatasetInfo:
    code: str
    name: str
    provider_code: str
    description: Optional[str]


@dataclass
class SeriesInfo:
    series_code: str
    provider_code: str
    dataset_code: str
    name: Optional[str]
    metadata: Dict[str, Any]


class DBNomicsClient:
    """Typed helper for the DB.nomics REST API with minimal retry support."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._client = httpx.Client(timeout=self.timeout)

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.request(method, url, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPError as exc:  # pragma: no cover - transient failures
                if attempt >= self.max_retries:
                    raise
                sleep_for = self.backoff_factor * (2**attempt)
                time.sleep(sleep_for)
        raise RuntimeError("Unreachable code after retries")

    def list_providers(self) -> List[ProviderInfo]:
        payload = self._request("GET", "/providers")
        providers = []
        for item in payload.get("providers", []):
            providers.append(
                ProviderInfo(
                    code=item.get("code"),
                    name=item.get("name", item.get("code")),
                    description=item.get("description"),
                )
            )
        return providers

    def list_datasets(self, provider_code: str) -> List[DatasetInfo]:
        payload = self._request("GET", f"/providers/{provider_code}/datasets")
        datasets: List[DatasetInfo] = []
        for item in payload.get("datasets", []):
            datasets.append(
                DatasetInfo(
                    code=item.get("code"),
                    name=item.get("name", item.get("code")),
                    provider_code=provider_code,
                    description=item.get("description"),
                )
            )
        return datasets

    def search_series(
        self,
        query: str,
        provider_code: Optional[str] = None,
        dataset_code: Optional[str] = None,
        limit: int = 100,
    ) -> List[SeriesInfo]:
        params: Dict[str, Any] = {"q": query, "limit": limit}
        if provider_code:
            params["provider"] = provider_code
        if dataset_code:
            params["dataset"] = dataset_code
        payload = self._request("GET", "/series/search", params=params)
        series: List[SeriesInfo] = []
        for item in payload.get("series", []):
            provider = item.get("provider_code") or provider_code or ""
            dataset = item.get("dataset_code") or dataset_code or ""
            series.append(
                SeriesInfo(
                    series_code=item.get("series_code") or item.get("code"),
                    provider_code=provider,
                    dataset_code=dataset,
                    name=item.get("name"),
                    metadata=item.get("metadata", {}),
                )
            )
        return series

    def fetch_series(self, series_code: str, frequency: Optional[str] = None) -> pd.DataFrame:
        params: Dict[str, Any] = {}
        if frequency:
            params["frequency"] = frequency
        payload = self._request("GET", f"/series/{series_code}", params=params)
        observations = payload.get("series", {}).get("observations", [])
        metadata = payload.get("series", {}).get("metadata", {})
        rows = []
        for obs in observations:
            date_val = obs.get("period") or obs.get("date")
            value = obs.get("value")
            rows.append(
                {
                    "provider": payload.get("series", {}).get("provider_code"),
                    "dataset": payload.get("series", {}).get("dataset_code"),
                    "series_code": payload.get("series", {}).get("series_code") or series_code,
                    "date": pd.to_datetime(date_val).date() if date_val else None,
                    "value": value,
                    "metadata": metadata,
                }
            )
        return pd.DataFrame(rows)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "DBNomicsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - trivial
        self.close()
