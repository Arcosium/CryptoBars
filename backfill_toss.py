#!/usr/bin/env python3
"""토스증권 Open API 1분봉 백필 — KIS(250거래일 롤링) 이전 구간을 메운다.

토스 국내 분봉은 2022-11-23 부터 진짜다(공식 FAQ. 2026-09-29 실측: 그 이전은 웹 차트에 응답은 오지만
1분 거래량 합이 일봉의 2~3%, 2022-11-23 부터 99.8%). KIS 가 전종목 2025-09-08 부터 있으므로 그 직전까지만 받는다.

⚠️ ts 는 토스 표기 그대로(봉 **종료** 시각, YYYYMMDDHHMMSS)다. KIS(bars_ohlc.db)는 봉 **시작** 시각이라
   이어 붙일 때 1분 당겨야 한다. 종가 단일가 봉은 예외가 있어 변환하지 않고 원표기로 남긴다(2026-09-29 실측):
   - NXT 이전·NXT 미거래 종목: 15:30 봉이 종가 단일가(=KIS 153000), 15:21~15:29 는 거래량 0 채움봉
   - NXT 거래 종목(2025-03-04~): 15:31 봉이 종가 단일가(005930 2025-09-10 1,050,560주 = KIS 153000)
   1분 당김으로 KIS 와 OHLCV 100% 일치(035720·000020 2025-09-10), 웹 차트와도 100% 일치(2023-06-15 3종목).
⚠️ 공식 API 는 KRX+NXT 통합 시세다(거래소 지정 불가). 2025-03-04 이후 NXT 거래 종목은 KIS(KRX 단독)와
   거래량이 다르다(005930 일치 1/381). 일부 종목(005930)은 NXT 이전 날짜도 08:01~20:00 채움봉 720개가 온다.
   그래서 bars_ohlc.db 에 섞지 않고 bars_toss.db 로 따로 둔다. 거래량 0 채움봉은 저장하지 않는다(ffill 로 복원).

상폐 종목은 404 — done 에 'notfound' 로 남긴다(생존편향 한계, 2022-11~2025-09 상폐분은 못 메운다).
재시작 안전: 종목 하나를 다 받은 뒤 한 트랜잭션에 쓰고 done 에 남긴다.
client 당 유효 토큰은 1개(재발급하면 이전 것 즉시 무효) → vault 캐시 파일을 공유하고 401 일 때만 재발급한다.
허용 IP 에서만 호출된다 — 아니면 403 'IP address not allowed' 로 즉시 멈춘다.
받은 데이터는 본인 매매 목적 한정·제3자 배포 금지(토스 약관) — 공개 복사본·영상에 쓰지 말 것.

사용:  python3 backfill_toss.py [--codes N] [--workers 3] [--rps 18]
"""
import argparse, csv, gzip, http.client, json, os, sqlite3, threading, time
import urllib.error, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ENV = '/home/arcosium/vault/CryptoBars/.env'
TOKEN_FILE = '/home/arcosium/vault/CryptoBars/.toss_token.json'
OUT = '/home/arcosium/vault/CryptoBars/data/KRX/bars_toss.db'
UNIVERSE = '/home/arcosium/vault/CryptoBars/data/KRX/universe_all.csv'
API = 'https://openapi.tossinvest.com'
UA = {'User-Agent': 'CryptoBars/1.0'}


class Fatal(Exception):
    """계속해 봐야 전부 실패할 오류(허용 IP·인증)."""


def _json(raw):
    # 토스는 Accept-Encoding 없이도 gzip 으로 줄 때가 있다(403 본문이 그랬다)
    return json.loads(gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw)


_lock, _tok = threading.Lock(), {}


def token(stale=None):
    """캐시 토큰. stale(401 받은 토큰)과 같을 때만 재발급 — 스레드마다 재발급하면 서로를 무효화한다."""
    with _lock:
        if not _tok:
            try:
                _tok.update(json.load(open(TOKEN_FILE)))
            except (OSError, ValueError):
                pass
        if _tok.get('access_token') not in (None, stale) and _tok.get('exp', 0) > time.time():
            return _tok['access_token']
        env = dict(l.split('=', 1) for l in open(ENV).read().splitlines() if '=' in l and not l.startswith('#'))
        body = urllib.parse.urlencode({'grant_type': 'client_credentials', 'client_id': env['TOSS_CLIENT_ID'],
                                       'client_secret': env['TOSS_CLIENT_SECRET']}).encode()
        try:
            with urllib.request.urlopen(urllib.request.Request(API + '/oauth2/token', body, UA), timeout=30) as r:
                j = _json(r.read())
        except urllib.error.HTTPError as e:
            raise Fatal(f'토큰 {e.code} {_json(e.read())}') from None
        _tok.clear()
        _tok.update(access_token=j['access_token'], exp=time.time() + int(j.get('expires_in', 86400)) - 300)
        with os.fdopen(os.open(TOKEN_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as f:
            json.dump(_tok, f)
        return _tok['access_token']


class Pace:
    """전 스레드 합산 초당 rps 회."""
    def __init__(self, rps):
        self.gap, self.next, self.lock = 1 / rps, 0.0, threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next)
            self.next = slot + self.gap
        time.sleep(slot - now)


def get(q, pace):
    """캔들 한 페이지. 404(상폐·없는 종목)면 None."""
    tok = token()
    for attempt in range(6):
        pace.wait()
        req = urllib.request.Request(API + '/api/v1/candles?' + urllib.parse.urlencode(q),
                                     headers={**UA, 'Authorization': 'Bearer ' + tok})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return _json(r.read())['result']
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 401:
                tok = token(stale=tok)
                continue
            if e.code == 403:
                raise Fatal(f'403 {_json(e.read())}') from None
            if e.code != 429 and e.code < 500:
                raise RuntimeError(f'{e.code} {e.read()[:200]!r}') from None
        # 응답이 중간에 끊기면 JSON 이 잘린다(2026-09-30 041830: Unterminated string, char 2999) — 재시도 대상
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException, ValueError):
            pass
        time.sleep(2 ** attempt)
    raise RuntimeError('재시도 초과')


def fetch(code, start, end, pace):
    """end 에서 과거로 페이지를 넘기며 start(YYYYMMDD) 까지. 반환 (rows, pages) 또는 None(404)."""
    rows, before, pages = {}, end, 0
    while before:
        res = get({'symbol': code, 'interval': '1m', 'count': 200, 'before': before, 'adjusted': 'false'}, pace)
        if res is None:
            return None
        pages += 1
        for x in res['candles']:
            ts = x['timestamp'][:19].replace('-', '').replace('T', '').replace(':', '')
            if ts[:8] < start:
                return list(rows.values()), pages
            if x['volume'] in ('0', 0):  # 체결 없는 분의 채움봉(직전가 반복) — KIS 처럼 저장하지 않는다
                continue
            rows[ts] = (code, ts, float(x['openPrice']), float(x['highPrice']), float(x['lowPrice']),
                        float(x['closePrice']), float(x['volume']))
        nxt = res.get('nextBefore')
        before = nxt if res['candles'] and nxt != before else None  # 제자리 커서면 무한루프 대신 종료
    return list(rows.values()), pages


def open_out(path):
    c = sqlite3.connect(path, timeout=60)
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('''CREATE TABLE IF NOT EXISTS bars(
        code TEXT NOT NULL, ts TEXT NOT NULL,
        o REAL, h REAL, l REAL, c REAL, v REAL,
        PRIMARY KEY(code, ts))''')
    c.execute('CREATE TABLE IF NOT EXISTS done(code TEXT PRIMARY KEY, status TEXT)')
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--codes', type=int, default=0, help='앞에서 N종목만 (0=전체)')
    ap.add_argument('--workers', type=int, default=3)
    ap.add_argument('--rps', type=float, default=18, help='공식 한도 초당 20회')
    ap.add_argument('--start', default='20221123')
    ap.add_argument('--end', default='2025-09-07T23:59:59+09:00', help='KIS 가 2025-09-08 부터 있다')
    ap.add_argument('--out', default=OUT)
    a = ap.parse_args()

    with open(UNIVERSE, newline='') as f:
        codes = [r['code'] for r in csv.DictReader(f)]
    if a.codes:
        codes = codes[:a.codes]
    conn = open_out(a.out)
    have = {r[0] for r in conn.execute('SELECT code FROM done')}
    todo = [c for c in codes if c not in have]
    print(f'종목 {len(codes)} 중 남은 {len(todo)}  {a.start}~{a.end[:10]}  workers {a.workers} rps {a.rps}', flush=True)

    pace, t0, n, bars = Pace(a.rps), time.time(), 0, 0
    token()  # 허용 IP·키 문제면 여기서 바로 멈춘다
    with ThreadPoolExecutor(a.workers) as ex:
        futs = {ex.submit(fetch, c, a.start, a.end, pace): c for c in todo}
        for f in as_completed(futs):
            # pop: 끝난 Future 는 결과(종목당 봉 수십만 개)를 쥐고 있다. dict 에 남겨 두면 종목마다 ~60MB 씩
            # 쌓여 전종목이면 100GB 를 넘는다(2026-09-29 27종목에 RSS 1.8GB 로 발견) — 통합메모리라 기기가 멎는다.
            code, n = futs.pop(f), n + 1
            try:
                got = f.result()
            except Fatal:
                ex.shutdown(cancel_futures=True)
                raise
            except Exception as e:
                print(f'  {code} 실패 {type(e).__name__} {e}'[:200], flush=True)
                continue
            if got is None:
                conn.execute('INSERT OR REPLACE INTO done VALUES(?,?)', (code, 'notfound'))
                msg = '404(상폐?)'
            else:
                rows, pages = got
                conn.executemany('INSERT OR IGNORE INTO bars VALUES(?,?,?,?,?,?,?)', rows)
                conn.execute('INSERT OR REPLACE INTO done VALUES(?,?)', (code, str(len(rows))))
                bars += len(rows)
                msg = f'{len(rows):>7,}봉 {pages:>4}쪽 {rows[-1][1][:8] if rows else "-"}~'
            conn.commit()
            el = time.time() - t0
            print(f'{n:>4}/{len(todo)} {code} {msg}  누적 {bars:,}봉  '
                  f'경과 {el/3600:.1f}h 남은 {el/n*(len(todo)-n)/3600:.1f}h', flush=True)


if __name__ == '__main__':
    main()
