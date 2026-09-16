# CryptoBars — KRX·NXT·크립토 분봉 수집기

24/7 백그라운드로 **크립토 perp 전 종목(776개)** 의 1분봉을 모아 Parquet 로 쌓는다.
프론트엔드 없음, 매매 없음, 크립토는 공개 API를 쓰고 NXT 시세는 vault에 보관한 KIS 자격증명을 사용한다. 주문 API는 호출하지 않는다.

QuantInSight 의 KRX 분봉 크롤러(`market_bars/`)와 **같은 원칙**으로 만들었다 —
매분 증분 수집 + 최근 몇 분 재조회로 자가 치유, 어떤 예외도 루프를 안 죽임, 영구 보존.
KRX의 기존 수집 로직은 `equities.py`가 불러 실행한다. `cryptobars.service`가 크립토와 국내 주식 수집을 함께 관리하며 QuantInSight 내부 수집기는 기본 비활성이다.

## 통합 저장 경로

`data`는 `/home/arcosium/vault/CryptoBars/data`를 가리키는 심링크다. ArcTrade의 `minute_data`도 같은 곳을 읽는다.

| 경로 | 데이터 |
|---|---|
| `KRX/bars.db` | 기존 KRX 종가·누적 거래량 분봉, 원본 보존 |
| `KRX/universe.csv` | 기존 국내 주식 유니버스 |
| `NXT/bars.db` | KIS NX 시세의 완성 OHLCV 분봉 |
| `bars/`, `history/`, `export/` | 기존 크립토 Parquet와 내보내기 |
| `USA/` | ArcTrade 조회 시 갱신하는 미국 일봉 캐시 |
| `equities_status.json` | KRX·NXT 수집 상태 |

QuantInSight의 기존 `data/bars.db` 경로도 KRX 파일로 연결한다. 수집기는 하나만 실행하며 기존 조회 코드는 같은 자료를 계속 읽는다. KRX의 종가만 있는 과거 봉을 임의의 OHLC로 바꾸지 않는다. NXT는 국내 유니버스 중 실제 시세가 반환되는 종목을 수집하며 없는 봉을 만들지 않는다. 최근 봉부터 마지막 저장 시각까지 거슬러 조회해 당일 중단 구간을 복구한다. API가 제공하지 않는 이전 거래일의 누락 구간은 임의로 채우지 않는다.

## 구성

| 파일 | 역할 |
|---|---|
| `venues.py` | 거래소 6곳(binance·bybit·backpack·paradex·grvt·hyperliquid)의 상장목록·1분봉을 공통 스키마로 정규화 |
| `equities.py` | KRX 기존 수집기 실행 · NXT 전용 분봉 저장 · 중복 실행 방지 |
| `collector.py` | 유니버스 선정 · 수집 루프 · Parquet 저장 · 조회 헬퍼 · CLI |
| `test_collector.py` | 네트워크 없이 도는 자체점검 |
| `cryptobars.service` | systemd 유닛 |

## 종목 1개 = 거래소 1곳

같은 코인을 6곳에서 중복 수집하면 용량만 6배가 되고 가격은 사실상 같다.
그래서 base 별로 **24h 거래대금이 가장 큰 거래소 한 곳**만 본다(6시간마다 재평가).

현재 배정: binance 526 · bybit 201 · grvt 37 · hyperliquid 8 · paradex 4 = **776종목**

거래소간 스프레드가 필요하면 그건 MultiEX 의 capture lane 이 할 일이다.

## 실행

```bash
.venv/bin/python collector.py                      # 상시 수집(데몬)
.venv/bin/python collector.py --once               # 1사이클만 (검증)
.venv/bin/python collector.py --refresh-universe   # 유니버스만 갱신
.venv/bin/python collector.py --stats              # 쌓인 데이터 요약
.venv/bin/python test_collector.py                 # 자체점검
```

서비스:

```bash
sudo systemctl status  cryptobars.service
sudo systemctl restart cryptobars.service
journalctl -u cryptobars.service -f
```

## 저장 형식

정본은 `data/history` 하나다. 수집기는 `data/bars` 에 먼저 쌓고, 하루가 지나면
`compact.py` 가 그날 치를 종목별 월 파일로 접어 넣은 뒤 버퍼를 지운다.
그래서 **3년치 위에 실시간 수집분이 계속 얹힌다.**

```
data/history/base=<BASE>/part-<YYYY-MM>.parquet   # 정본 (2023-01-01 ~ 계속)
data/bars/date=YYYY-MM-DD/part-<UTC>.parquet      # 오늘 치 버퍼 (내일 접힌다)
data/universe.csv                                 # base → 거래소 배정 (감사용)
data/history_universe.csv                         # 백필 대상 (상장폐지 포함)
```

compaction 은 수집기가 UTC 날짜가 바뀔 때 스레드로 돌린다(수집 루프를 막지 않는다).
`CRYPTOBARS_COMPACT=0` 으로 끌 수 있고, `python compact.py` 로 수동 실행도 된다.
**백필이 도는 동안엔 자동으로 건너뛴다** — 같은 월 파일을 양쪽에서 쓰면 한쪽이 사라진다.

컬럼: `ts`(봉 시작 UTC epoch ms) · `venue` · `base` · `symbol` · `open` `high` `low` `close`
· `volume`(base 수량) · `quote_volume`(달러, 모르면 null)

날짜 파티션은 **UTC 기준**이다. 자동 삭제 없음 — 지나간 분봉은 다시 살 수 없다.

part 파일끼리는 **중복이 남는다**(매 사이클 최근 5분을 다시 받고, 재시작 때 4시간을 되받으므로).
읽을 때 걸러라:

```python
from collector import read_bars
df = read_bars("base = 'BTC'")     # (ts, base) 중복 제거된 DataFrame
```

## 실측 (2026-08-05)

- 기동 백필 240분: 776종목 **184,530행 / 21초 / 실패 0**
- Parquet **22.5 bytes/행** → 하루 약 112만행 ≈ **25MB/일**, 연 9GB

## 튜닝 (환경변수)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `CRYPTOBARS_DATA` | `./data` | 저장 루트 |
| `CRYPTOBARS_VENUES` | 6곳 전부 | 쓸 거래소 (쉼표) |
| `CRYPTOBARS_LOOKBACK` | 5 | 매 사이클 되받는 봉 수 |
| `CRYPTOBARS_BACKFILL` | 240 | 기동 시 1회 백필(분) |
| `CRYPTOBARS_FLUSH_MINUTES` | 10 | Parquet flush 주기 |
| `CRYPTOBARS_UNIVERSE_TTL_H` | 6 | 유니버스 재평가 주기 |
| `CRYPTOBARS_CYCLE_SECOND` | 8 | 매분 몇 초에 수집할지 |

## 알아둘 것

- **유니버스에 토큰화 주식·원자재가 섞여 있다** — `UBER` `AMD` `DIS` `NOK` `EWZ` `NVO` `SPACEX`
  `OPENAI` `ANTHROPIC` `NATGAS` `XPT`(백금) `COPPER` `URNM` 등. paradex/backpack 이 상장한
  것들로, 코인이 아니다. 순수 코인만 원하면 `data/universe.csv` 에서 빼면 된다.
- **`quote_volume` 은 hyperliquid·paradex 에서 null** 이다. 거래소가 안 준다 —
  `close × volume` 으로 지어내지 않는다.
- 코인 quote perp(`ETHBTC` 등)는 제외한다. 거래대금 단위가 달러가 아니라 비교가 안 된다.
