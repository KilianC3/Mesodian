"""Reference data endpoints."""

from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.engine import get_db
from app.db.models import Asset, Country, Indicator
from app.pools.loader import get_pool_tickers, load_equity_pools


router = APIRouter()


@router.get("/reference/countries")
def list_countries(db: Session = Depends(get_db)) -> Dict[str, List[Dict]]:
    countries = (
        db.query(Country)
        .filter(Country.id.in_(COUNTRY_UNIVERSE))
        .order_by(Country.id)
        .all()
    )
    return {
        "countries": [
            {
                "id": country.id,
                "name": country.name,
                "region": country.region,
                "income_group": country.income_group,
            }
            for country in countries
        ]
    }


@router.get("/reference/indicators")
def list_indicators(
    source: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> Dict[str, List[Dict]]:
    query = db.query(Indicator)
    if source:
        query = query.filter(Indicator.source == source)
    if category:
        query = query.filter(Indicator.category == category)

    indicators = query.order_by(Indicator.id).all()
    return {
        "indicators": [
            {
                "id": indicator.id,
                "source": indicator.source,
                "source_code": indicator.source_code,
                "canonical_code": indicator.canonical_code,
                "frequency": indicator.frequency,
                "unit": indicator.unit,
                "category": indicator.category,
            }
            for indicator in indicators
        ]
    }


@router.get("/reference/assets")
def list_assets(
    country_id: Optional[str] = Query(default=None),
    asset_type: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> Dict[str, List[Dict]]:
    query = db.query(Asset)
    if country_id:
        query = query.filter(func.upper(Asset.country_id) == country_id.upper())
    if asset_type:
        query = query.filter(Asset.asset_type == asset_type)

    assets = query.order_by(Asset.symbol).all()
    return {
        "assets": [
            {
                "id": asset.id,
                "symbol": asset.symbol,
                "name": asset.name,
                "asset_type": asset.asset_type,
                "country_id": asset.country_id,
                "region": asset.region,
            }
            for asset in assets
        ]
    }


@router.get("/reference/equity-pools")
def list_equity_pools() -> Dict[str, List[Dict]]:
    pools = load_equity_pools()
    return {
        "pools": [
            {
                "name": name,
                "description": config.get("description"),
                "tickers": list(config.get("tickers", [])),
            }
            for name, config in pools.items()
        ]
    }


@router.get("/reference/equity-pools/{pool_name}")
def get_equity_pool(pool_name: str) -> Dict[str, Dict]:
    pools = load_equity_pools()
    if pool_name not in pools:
        raise HTTPException(status_code=404, detail="Pool not found")

    tickers = get_pool_tickers(pool_name)
    pool = pools[pool_name]
    return {
        "pool": {
            "name": pool_name,
            "description": pool.get("description"),
            "tickers": tickers,
        }
    }


reference_router = router
