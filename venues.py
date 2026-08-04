"""거래소 6곳의 ① 상장 perp 목록 ② 1분봉 조회를 공통 스키마로 정규화.

정규화 결과:
  listing → {venue, symbol, base, quote, volume_24h_usd}
  kline   → {ts, venue, base, symbol, open, high, low, close, volume, quote_volume}
            ts = 봉 시작시각(UTC epoch ms). volume=base 수량, quote_volume=달러 환산(모르면 None).

거래소마다 응답 모양이 제각각이라(순서·정렬·시간단위) 여기서만 흡수하고 밖으로는 위 스키마만 낸다.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

REST = {
    "binance": "https://fapi.binance.com",
    "bybit": "https://api.bybit.com",
    "backpack": "https://api.backpack.exchange",
    "paradex": "https://api.prod.paradex.trade",
    "grvt": "https://market-data.grvt.io",
    "hyperliquid": "https://api.hyperliquid.xyz",
}

# 달러 계열 quote. 코인 quote(ETHBTC 등)는 거래대금 단위가 달라 섞으면 안 된다.
USD_QUOTES = {"USDT", "USDC", "USD", "USD1", "U"}

VENUES = tuple(REST)


def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def client(timeout=20.0) -> httpx.Client:
    return httpx.Client(timeout=timeout, follow_redirects=True,
                        headers={"User-Agent": "CryptoBars/1.0"})


# ── ① 상장 목록 ──────────────────────────────────────────────────────────
def list_binance(c):
    vol = {t["symbol"]: _f(t.get("quoteVolume")) for t in c.get(f"{REST['binance']}/fapi/v1/ticker/24hr").json()}
    for s in c.get(f"{REST['binance']}/fapi/v1/exchangeInfo").json()["symbols"]:
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            yield {"venue": "binance", "symbol": s["symbol"], "base": s["baseAsset"],
                   "quote": s["quoteAsset"], "volume_24h_usd": vol.get(s["symbol"], 0.0)}


def list_bybit(c):
    vol = {t["symbol"]: _f(t.get("turnover24h")) for t in
           c.get(f"{REST['bybit']}/v5/market/tickers", params={"category": "linear"}).json()["result"]["list"]}
    cursor = ""
    while True:
        p = {"category": "linear", "limit": 1000}
        if cursor:
            p["cursor"] = cursor
        res = c.get(f"{REST['bybit']}/v5/market/instruments-info", params=p).json()["result"]
        for s in res["list"]:
            if s.get("status") == "Trading" and s.get("contractType") == "LinearPerpetual":
                yield {"venue": "bybit", "symbol": s["symbol"], "base": s["baseCoin"],
                       "quote": s["quoteCoin"], "volume_24h_usd": vol.get(s["symbol"], 0.0)}
        cursor = res.get("nextPageCursor") or ""
        if not cursor:
            return


def list_backpack(c):
    vol = {t["symbol"]: _f(t.get("quoteVolume")) for t in c.get(f"{REST['backpack']}/api/v1/tickers").json()}
    for m in c.get(f"{REST['backpack']}/api/v1/markets").json():
        if m.get("marketType") == "PERP" and m.get("orderBookState") in (None, "Open"):
            yield {"venue": "backpack", "symbol": m["symbol"], "base": m["baseSymbol"],
                   "quote": m["quoteSymbol"], "volume_24h_usd": vol.get(m["symbol"], 0.0)}


def list_paradex(c):
    summ = {s["symbol"]: s for s in
            c.get(f"{REST['paradex']}/v1/markets/summary", params={"market": "ALL"}).json()["results"]}
    for m in c.get(f"{REST['paradex']}/v1/markets").json()["results"]:
        if m.get("asset_kind") in ("PERP", "PERPETUAL"):
            yield {"venue": "paradex", "symbol": m["symbol"], "base": m.get("base_currency", ""),
                   "quote": m.get("quote_currency", ""),
                   "volume_24h_usd": _f(summ.get(m["symbol"], {}).get("volume_24h"))}


def list_grvt(c):
    for i in c.post(f"{REST['grvt']}/full/v1/instruments",
                    json={"kind": ["PERPETUAL"], "is_active": True}).json().get("result", []):
        sym = i.get("instrument", "")
        # 목록 API 에 24h 거래대금이 없어 instrument 별 ticker 를 한 번 더 친다.
        t = c.post(f"{REST['grvt']}/full/v1/ticker", json={"instrument": sym}).json().get("result") or {}
        yield {"venue": "grvt", "symbol": sym, "base": i.get("base", ""), "quote": i.get("quote", ""),
               "volume_24h_usd": _f(t.get("buy_volume_24h_q")) + _f(t.get("sell_volume_24h_q"))}


def list_hyperliquid(c):
    meta, ctxs = c.post(f"{REST['hyperliquid']}/info", json={"type": "metaAndAssetCtxs"}).json()
    for a, ctx in zip(meta["universe"], ctxs):
        if not a.get("isDelisted"):
            yield {"venue": "hyperliquid", "symbol": a["name"], "base": a["name"], "quote": "USD",
                   "volume_24h_usd": _f(ctx.get("dayNtlVlm"))}


LISTERS = {"binance": list_binance, "bybit": list_bybit, "backpack": list_backpack,
           "paradex": list_paradex, "grvt": list_grvt, "hyperliquid": list_hyperliquid}


# ── ② 1분봉 ─────────────────────────────────────────────────────────────
def _row(ts, venue, base, symbol, o, h, l, c_, v, qv):
    return {"ts": int(ts), "venue": venue, "base": base, "symbol": symbol,
            "open": _f(o), "high": _f(h), "low": _f(l), "close": _f(c_),
            "volume": _f(v), "quote_volume": None if qv is None else _f(qv)}


def kl_binance(c, sym, base, limit, now_ms):
    r = c.get(f"{REST['binance']}/fapi/v1/klines",
              params={"symbol": sym, "interval": "1m", "limit": limit}).json()
    return [_row(k[0], "binance", base, sym, k[1], k[2], k[3], k[4], k[5], k[7]) for k in r]


def kl_bybit(c, sym, base, limit, now_ms):
    r = c.get(f"{REST['bybit']}/v5/market/kline",
              params={"category": "linear", "symbol": sym, "interval": "1", "limit": limit}).json()
    return [_row(k[0], "bybit", base, sym, k[1], k[2], k[3], k[4], k[5], k[6])
            for k in r["result"]["list"]]


def kl_backpack(c, sym, base, limit, now_ms):
    start = now_ms // 1000 - limit * 60
    r = c.get(f"{REST['backpack']}/api/v1/klines",
              params={"symbol": sym, "interval": "1m", "startTime": start}).json()
    out = []
    for k in r:
        ts = int(datetime.strptime(k["start"], "%Y-%m-%d %H:%M:%S")
                 .replace(tzinfo=timezone.utc).timestamp() * 1000)
        out.append(_row(ts, "backpack", base, sym, k["open"], k["high"], k["low"], k["close"],
                        k["volume"], k.get("quoteVolume")))
    return out


def kl_hyperliquid(c, sym, base, limit, now_ms):
    r = c.post(f"{REST['hyperliquid']}/info",
               json={"type": "candleSnapshot",
                     "req": {"coin": sym, "interval": "1m",
                             "startTime": now_ms - limit * 60_000, "endTime": now_ms}}).json()
    # hyperliquid 는 quote_volume 을 안 준다 → None (close*volume 로 지어내지 않는다).
    return [_row(k["t"], "hyperliquid", base, sym, k["o"], k["h"], k["l"], k["c"], k["v"], None)
            for k in r]


def kl_paradex(c, sym, base, limit, now_ms):
    r = c.get(f"{REST['paradex']}/v1/markets/klines",
              params={"symbol": sym, "resolution": 1,
                      "start_at": now_ms - limit * 60_000, "end_at": now_ms}).json()
    return [_row(k[0], "paradex", base, sym, k[1], k[2], k[3], k[4], k[5], None)
            for k in r.get("results", [])]


def kl_grvt(c, sym, base, limit, now_ms):
    r = c.post(f"{REST['grvt']}/full/v1/kline",
               json={"instrument": sym, "interval": "CI_1_M", "type": "TRADE", "limit": limit}).json()
    # open_time 은 **나노초 문자열**이다.
    return [_row(int(k["open_time"]) // 1_000_000, "grvt", base, sym,
                 k["open"], k["high"], k["low"], k["close"], k.get("volume_b"), k.get("volume_q"))
            for k in r.get("result", [])]


KLINES = {"binance": kl_binance, "bybit": kl_bybit, "backpack": kl_backpack,
          "paradex": kl_paradex, "grvt": kl_grvt, "hyperliquid": kl_hyperliquid}

# 분당 요청 수 상한이 거래소마다 달라 동시 워커 수로 조절한다(IP 차단이 곧 수집 중단).
WORKERS = {"binance": 12, "bybit": 8, "backpack": 4, "paradex": 4, "grvt": 6, "hyperliquid": 2}
