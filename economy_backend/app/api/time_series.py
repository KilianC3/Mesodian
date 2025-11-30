"""Time series endpoints."""

import datetime as dt
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.engine import get_db
from app.db.models import Asset, AssetPrice, Indicator, TimeSeriesValue


router = APIRouter()


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD") from exc


def _to_float(value):
    return float(value) if value is not None else None


@router.get("/timeseries/country/{country_id}")
def country_timeseries(
    country_id: str,
    indicator_codes: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> Dict:
    country_id = country_id.upper()
    if country_id not in COUNTRY_UNIVERSE:
        raise HTTPException(status_code=404, detail="Country not supported")

    start = _parse_date(start_date)
    end = _parse_date(end_date)

    code_list: Optional[List[str]] = None
    if indicator_codes:
        code_list = [code.strip() for code in indicator_codes.split(",") if code.strip()]

    query = (
        db.query(Indicator, TimeSeriesValue)
        .join(TimeSeriesValue, Indicator.id == TimeSeriesValue.indicator_id)
        .filter(TimeSeriesValue.country_id == country_id)
    )
    if code_list:
        query = query.filter(Indicator.canonical_code.in_(code_list))
    if start:
        query = query.filter(TimeSeriesValue.date >= start)
    if end:
        query = query.filter(TimeSeriesValue.date <= end)

    results = query.order_by(TimeSeriesValue.date.asc()).all()

    series_map: Dict[str, Dict] = {}
    for indicator, value in results:
        code = indicator.canonical_code or indicator.source_code
        entry = series_map.setdefault(
            code,
            {
                "indicator_code": code,
                "indicator_name": indicator.source_code,
                "frequency": indicator.frequency,
                "values": [],
            },
        )
        entry["values"].append({"date": value.date.isoformat(), "value": _to_float(value.value)})

    return {"country_id": country_id, "series": list(series_map.values())}


@router.get("/timeseries/indicator/{canonical_code}")
def indicator_timeseries(
    canonical_code: str,
    country_id: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> Dict:
    indicator = (
        db.query(Indicator)
        .filter(func.upper(Indicator.canonical_code) == canonical_code.upper())
        .first()
    )
    if not indicator:
        raise HTTPException(status_code=404, detail="Indicator not found")

    start = _parse_date(start_date)
    end = _parse_date(end_date)

    countries: List[str]
    if country_id:
        country_id = country_id.upper()
        if country_id not in COUNTRY_UNIVERSE:
            raise HTTPException(status_code=404, detail="Country not supported")
        countries = [country_id]
    else:
        countries = COUNTRY_UNIVERSE

    query = db.query(TimeSeriesValue).filter(TimeSeriesValue.indicator_id == indicator.id)
    query = query.filter(TimeSeriesValue.country_id.in_(countries))
    if start:
        query = query.filter(TimeSeriesValue.date >= start)
    if end:
        query = query.filter(TimeSeriesValue.date <= end)

    results = query.order_by(TimeSeriesValue.country_id, TimeSeriesValue.date.asc()).all()

    series_map: Dict[str, List[Dict]] = {country: [] for country in countries}
    for value in results:
        series_map.setdefault(value.country_id, []).append(
            {"date": value.date.isoformat(), "value": _to_float(value.value)}
        )

    return {
        "indicator_code": indicator.canonical_code or indicator.source_code,
        "series": [
            {"country_id": cid, "values": values}
            for cid, values in series_map.items()
            if values
        ],
    }


@router.get("/timeseries/asset/{symbol}")
def asset_prices(
    symbol: str,
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    include_meta: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> Dict:
    asset = (
        db.query(Asset)
        .filter(func.lower(Asset.symbol) == symbol.lower())
        .first()
    )
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    start = _parse_date(start_date)
    end = _parse_date(end_date)

    query = db.query(AssetPrice).filter(AssetPrice.asset_id == asset.id)
    if start:
        query = query.filter(AssetPrice.date >= start)
    if end:
        query = query.filter(AssetPrice.date <= end)

    prices = query.order_by(AssetPrice.date.asc()).all()

    payload = {
        "symbol": asset.symbol,
        "prices": [
            {
                "date": price.date.isoformat(),
                "open": _to_float(price.open),
                "high": _to_float(price.high),
                "low": _to_float(price.low),
                "close": _to_float(price.close),
                "adj_close": _to_float(price.adj_close),
                "volume": _to_float(price.volume),
            }
            for price in prices
        ],
    }
    if include_meta:
        payload["asset"] = {
            "id": asset.id,
            "name": asset.name,
            "asset_type": asset.asset_type,
            "country_id": asset.country_id,
            "region": asset.region,
        }
    return payload


@router.get("/timeseries/fx")
def fx_timeseries(
    base: str = Query(...),
    quote: str = Query(...),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> Dict:
    canonical_code = f"FX_{base.upper()}_{quote.upper()}"
    indicator = (
        db.query(Indicator)
        .filter(func.upper(Indicator.canonical_code) == canonical_code)
        .first()
    )
    if not indicator:
        raise HTTPException(status_code=404, detail="FX pair not found")

    start = _parse_date(start_date)
    end = _parse_date(end_date)

    query = db.query(TimeSeriesValue).filter(TimeSeriesValue.indicator_id == indicator.id)
    if start:
        query = query.filter(TimeSeriesValue.date >= start)
    if end:
        query = query.filter(TimeSeriesValue.date <= end)

    values = query.order_by(TimeSeriesValue.date.asc()).all()
    return {
        "indicator_code": canonical_code,
        "values": [
            {"date": value.date.isoformat(), "value": _to_float(value.value)}
            for value in values
        ],
    }


timeseries_router = router
