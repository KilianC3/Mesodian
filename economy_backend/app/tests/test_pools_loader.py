import pytest

from app.pools.loader import get_all_tickers, get_pool_tickers, load_equity_pools


def test_load_equity_pools_structure():
    pools = load_equity_pools()
    assert "global_benchmarks" in pools
    assert "us_tech_megacap" in pools
    assert pools["global_benchmarks"]["tickers"] == ["SPY", "ACWI", "VT"]


def test_get_pool_tickers_contains_expected_symbols():
    tickers = get_pool_tickers("us_tech_megacap")
    assert "AAPL" in tickers
    assert "MSFT" in tickers
    assert tickers[0] == "AAPL"


def test_get_all_tickers_union():
    all_tickers = get_all_tickers()
    for symbol in ["SPY", "QQQ", "AAPL", "XOM", "TSM", "ICLN"]:
        assert symbol in all_tickers
    assert len(all_tickers) == len(set(all_tickers))


def test_unknown_pool_error():
    with pytest.raises(KeyError):
        get_pool_tickers("does_not_exist")
