"""수집기 자체점검 — 네트워크 없이 돈다. `python3 test_collector.py` 또는 pytest.

거래소별 응답을 실제로 받아본 모양 그대로 박아두고 정규화 결과를 검증한다
(정렬 방향·시간 단위가 거래소마다 달라서 여기가 제일 잘 틀린다).
"""
import tempfile
from pathlib import Path

import venues
from collector import SCHEMA, Store


class FakeResp:
    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


class FakeClient:
    """get/post 어느 쪽으로 불려도 미리 정한 payload 를 준다."""

    def __init__(self, payload):
        self._p = payload

    def get(self, *a, **k):
        return FakeResp(self._p)

    def post(self, *a, **k):
        return FakeResp(self._p)


NOW = 1785877200000            # 2026-08-04T20:00:00Z


def test_kline_normalization():
    cases = [
        # (venue, 실제 응답 모양, 기대 ts, 기대 close)
        ("binance", [[1785877020000, "64291.5", "64310.0", "64291.5", "64309.9", "63.788",
                      1785877079999, "4101579.2", 1107, "49.9", "3211760.2", "0"]],
         1785877020000, 64309.9),
        ("bybit", {"result": {"list": [["1785877020000", "64297.7", "64315.5", "64297.7",
                                        "64314.4", "16.267", "1046067.5"]]}},
         1785877020000, 64314.4),
        ("hyperliquid", [{"t": 1785877020000, "T": 1785877079999, "o": "64315.0", "c": "64313.0",
                          "h": "64315.0", "l": "64313.0", "v": "1.57"}],
         1785877020000, 64313.0),
        ("paradex", {"results": [[1785877020000, 64305.8, 64307.0, 64297.3, 64297.3, 756]]},
         1785877020000, 64297.3),
        # grvt 의 open_time 은 나노초 문자열 — ms 로 접혀야 한다
        ("grvt", {"result": [{"open_time": "1785877020000000000", "open": "64304.3",
                              "high": "64304.3", "low": "64293.1", "close": "64293.1",
                              "volume_b": "0.048", "volume_q": "3086.6"}]},
         1785877020000, 64293.1),
        # backpack 의 start 는 UTC 문자열
        ("backpack", [{"start": "2026-08-04 20:57:00", "open": "64291.6", "high": "64291.6",
                       "low": "64291.6", "close": "64291.6", "volume": "0.001",
                       "quoteVolume": "60.4"}],
         1785877020000, 64291.6),
    ]
    for venue, payload, want_ts, want_close in cases:
        rows = venues.KLINES[venue](FakeClient(payload), "SYM", "BTC", 5, NOW)
        assert len(rows) == 1, f"{venue}: {rows}"
        r = rows[0]
        assert r["ts"] == want_ts, f"{venue}: ts {r['ts']} != {want_ts}"
        assert r["close"] == want_close, f"{venue}: close {r['close']} != {want_close}"
        assert r["venue"] == venue and r["base"] == "BTC"
        assert set(r) == {f.name for f in SCHEMA}, f"{venue}: 스키마 불일치 {set(r)}"


def test_store_dedupes_and_partitions_by_date():
    def row(ts, base, close):
        return {"ts": ts, "venue": "binance", "base": base, "symbol": base + "USDT",
                "open": close, "high": close, "low": close, "close": close,
                "volume": 1.0, "quote_volume": None}

    with tempfile.TemporaryDirectory() as d:
        s = Store(Path(d))
        s.add([row(NOW, "BTC", 1.0), row(NOW, "ETH", 2.0)])
        s.add([row(NOW, "BTC", 9.0)])                       # 같은 (ts, base) → 나중 값이 이긴다
        s.add([row(NOW - 86_400_000, "BTC", 3.0)])          # 전날 → 다른 파티션
        assert s.flush() == 3
        assert s.buf == {}
        dates = sorted(p.name for p in Path(d).iterdir())
        assert dates == ["date=2026-08-03", "date=2026-08-04"], dates

        import pyarrow.parquet as pq
        t = pq.read_table(Path(d) / "date=2026-08-04").to_pylist()
        assert {r["base"]: r["close"] for r in t} == {"BTC": 9.0, "ETH": 2.0}
        assert s.flush() == 0                               # 빈 버퍼는 파일을 만들지 않는다


def test_unclosed_candle_is_dropped():
    from collector import fetch_cycle
    payload = [[NOW - 60_000, "1", "1", "1", "1", "1", 0, "1", 0, "1", "1", "0"],   # 닫힘
               [NOW, "2", "2", "2", "2", "2", 0, "2", 0, "2", "2", "0"]]            # 진행중
    rows, errs = fetch_cycle(FakeClient(payload),
                             [{"venue": "binance", "symbol": "BTCUSDT", "base": "BTC"}], 5, NOW)
    assert errs == []
    assert [r["ts"] for r in rows] == [NOW - 60_000], rows


def test_base_of_strips_longest_quote_first():
    """'ADABUSD' 를 'ADAB'+USD 로 자르면 유니버스에 유령 종목이 생긴다(BUSD 를 USD 보다 먼저 봐야 함)."""
    from backfill import base_of
    for sym, want in [("BTCUSDT", "BTC"), ("BTCUSDC", "BTC"), ("ADABUSD", "ADA"),
                      ("1000LUNCBUSD", "1000LUNC"), ("BTCUSD", "BTC"),
                      ("1000000BABYDOGEUSDT", "1000000BABYDOGE"), ("WEIRD", "WEIRD")]:
        assert base_of(sym) == want, f"{sym} → {base_of(sym)} != {want}"


def test_month_helpers():
    from datetime import datetime, timezone
    from backfill import month_bounds, months
    assert months(datetime(2023, 11, 5, tzinfo=timezone.utc),
                  datetime(2024, 2, 1, tzinfo=timezone.utc)) == \
        ["2023-11", "2023-12", "2024-01", "2024-02"]
    lo, hi = month_bounds("2023-12")          # 연말 경계에서 다음 달 계산이 틀리기 쉽다
    assert datetime.fromtimestamp(lo / 1000, timezone.utc).strftime("%Y-%m-%d") == "2023-12-01"
    assert datetime.fromtimestamp(hi / 1000, timezone.utc).strftime("%Y-%m-%d") == "2024-01-01"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
