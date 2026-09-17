#!/usr/bin/env python3
"""시세 데이터 1회성 스냅샷 → Google Drive 'market-data-snapshot' 폴더.

배경: vault 일일 백업에서 CryptoBars/data 를 제외했다(아카이브가 105GB 로 터졌다).
근거는 "Polygon·KIS 로 다시 받을 수 있다"였는데 **그 전제가 깨졌다** —
Polygon 구독 취소로 한 달 뒤 소급이 2년으로 줄고, KIS 는 250거래일 롤링이라 이미 재수집 불가.
이제 이 데이터는 재현 불가능한 유일본이다.

설계(사장 지시 2026-09-17): **과거 구간은 불변**이라는 성질을 쓴다.
연도 단위로 끊어 올리고, 지난 연도는 한 번 올린 뒤 건드리지 않는다. 올해 조각만 갱신한다.
매일 전체를 백업하지 않으므로 vault 백업이 다시 터지는 일이 없다.

조각 판별은 내용 지문(파일 수·총 크기·최신 mtime)으로 한다. 지문이 같으면 건너뛴다.

사용:
  python3 snapshot_drive.py --source usa      # 연도별 조각
  python3 snapshot_drive.py --source krx      # sqlite 통째(1년치)
  python3 snapshot_drive.py --source crypto   # history·export 통째
  python3 snapshot_drive.py --source all --dry-run
"""
import os, sys, json, time, hashlib, argparse, subprocess, tempfile

DATA = '/home/arcosium/vault/CryptoBars/data'
STATE = os.path.join(DATA, 'snapshot_state.json')
FOLDER = 'market-data-snapshot'
REPO = '/home/arcosium/projects/ArcAI.ve'


def fingerprint(paths):
    """조각의 내용 지문 — 파일 수·총 바이트·최신 mtime. 과거 조각은 이 값이 고정된다."""
    n = total = newest = 0
    for p in paths:
        try:
            st = os.stat(p)
        except OSError:
            continue
        n += 1
        total += st.st_size
        newest = max(newest, int(st.st_mtime))
    return f'{n}:{total}:{newest}', total


def usa_pieces():
    """미국 1분봉을 연도별로 나눈다 — 파일명이 YYYY-MM.parquet 이라 그대로 끊긴다."""
    root = os.path.join(DATA, 'USA', '1m')
    years = {}
    for t in sorted(os.listdir(root)):
        d = os.path.join(root, t)
        if not os.path.isdir(d):
            continue
        for f in os.listdir(d):
            if f.endswith('.parquet'):
                years.setdefault(f[:4], []).append(os.path.join(d, f))
    return [(f'usa-1m-{y}', sorted(v)) for y, v in sorted(years.items())]


def krx_pieces():
    root = os.path.join(DATA, 'KRX')
    fs = [os.path.join(root, f) for f in ('bars_ohlc.db', 'universe.csv', 'universe_all.csv')
          if os.path.exists(os.path.join(root, f))]
    return [('krx-1m', fs)]


def crypto_pieces():
    out = []
    for name in ('history', 'export'):
        root = os.path.join(DATA, name)
        if not os.path.isdir(root):
            continue
        fs = [os.path.join(r, f) for r, _d, files in os.walk(root) for f in files]
        out.append((f'crypto-{name}', sorted(fs)))
    return out


SOURCES = {'usa': usa_pieces, 'krx': krx_pieces, 'crypto': crypto_pieces}


def load_state():
    try:
        return json.load(open(STATE))
    except (OSError, ValueError):
        return {}


def save_state(s):
    tmp = STATE + '.tmp'
    json.dump(s, open(tmp, 'w'), ensure_ascii=False)
    os.replace(tmp, STATE)


def archive_and_upload(name, paths, dry):
    """파케이는 이미 zstd 라 tar 로 묶기만 한다(-1 은 sqlite·메타데이터용 최소 압축)."""
    with tempfile.NamedTemporaryFile(suffix='.tar.zst', delete=False) as tf:
        tmp = tf.name
    try:
        listfile = tmp + '.list'
        with open(listfile, 'w') as f:
            f.write('\n'.join(os.path.relpath(p, DATA) for p in paths))
        subprocess.run(f"tar -C {DATA} -T {listfile} -cf - | zstd -1 -T0 -o {tmp} -f",
                       shell=True, check=True, timeout=7200, executable='/bin/bash')
        size = os.path.getsize(tmp)
        print(f'  아카이브 {size/1e9:.2f}GB', flush=True)
        if dry:
            return size, None
        sys.path.insert(0, os.path.join(REPO, 'clients'))
        import drive_mcp
        r = drive_mcp.upload_file(tmp, name=f'{name}.tar.zst', folder=FOLDER)
        print(f'  {r}', flush=True)
        return size, r
    finally:
        for p in (tmp, tmp + '.list'):
            try:
                os.remove(p)
            except OSError:
                pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--source', default='all', choices=['all', 'usa', 'krx', 'crypto'])
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--force', action='store_true', help='지문이 같아도 다시 올린다')
    a = ap.parse_args()

    state = load_state()
    names = list(SOURCES) if a.source == 'all' else [a.source]
    for src in names:
        for name, paths in SOURCES[src]():
            fp, raw = fingerprint(paths)
            old = state.get(name, {})
            if old.get('fingerprint') == fp and not a.force:
                print(f'{name:18} 변화 없음 — 건너뜀 ({len(paths):,}파일 {raw/1e9:.2f}GB)', flush=True)
                continue
            print(f'{name:18} {len(paths):,}파일 {raw/1e9:.2f}GB → 아카이브', flush=True)
            t0 = time.time()
            size, _r = archive_and_upload(name, paths, a.dry_run)
            if not a.dry_run:
                state[name] = {'fingerprint': fp, 'files': len(paths), 'raw': raw,
                               'archive': size, 'ts': int(time.time()),
                               'took_s': int(time.time() - t0)}
                save_state(state)
            print(f'  {(time.time()-t0)/60:.1f}분', flush=True)


if __name__ == '__main__':
    main()
