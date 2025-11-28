from pathlib import Path
from pathlib import Path
from typing import Any, Dict, List

import yaml

_CONFIG_PATH = Path(__file__).with_name("equity_pools.yml")


def _read_config() -> Dict[str, Dict[str, Any]]:
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(f"Equity pools config not found at {_CONFIG_PATH}")

    try:
        data = yaml.safe_load(_CONFIG_PATH.read_text())
    except yaml.YAMLError as exc:  # pragma: no cover - unexpected parse failure
        raise ValueError(f"Failed to parse equity pools config: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("Equity pools config must be a mapping of pool definitions.")

    for pool_name, config in data.items():
        if not isinstance(config, dict):
            raise ValueError(f"Pool '{pool_name}' must be a mapping of attributes.")
        tickers = config.get("tickers")
        if not isinstance(tickers, list) or not all(isinstance(t, str) for t in tickers):
            raise ValueError(f"Pool '{pool_name}' must define a tickers list of strings.")

    return data


def load_equity_pools() -> Dict[str, Dict[str, Any]]:
    """Load the equity pools configuration from the YAML file."""

    return _read_config()


def get_pool_tickers(pool_name: str) -> List[str]:
    pools = load_equity_pools()
    if pool_name not in pools:
        available = ", ".join(sorted(pools))
        raise KeyError(f"Pool '{pool_name}' not found. Available pools: {available}")
    tickers = pools[pool_name].get("tickers", [])
    return list(tickers)


def get_all_tickers() -> List[str]:
    pools = load_equity_pools()
    seen = set()
    ordered: List[str] = []
    for pool in pools.values():
        for ticker in pool.get("tickers", []):
            if ticker not in seen:
                seen.add(ticker)
                ordered.append(ticker)
    return ordered
