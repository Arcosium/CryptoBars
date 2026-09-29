#!/bin/bash
# 수집 상태 한 눈에 — 무거운 count(*) 를 쓰지 않는다.
# 2026-09-21 실사고: 12GB sqlite 에 count(*) 를 걸자 잠금 때문에 백필이 죽었다(당시 journal_mode=delete).
# 지금은 WAL 이라 공존하지만, 1.5억 행 집계는 몇 분이 걸려 상태 확인용으로 맞지 않다.
# 정확한 수치가 필요하면 각 finalize 가 남긴 verify_report.json 을 본다.
V=/home/arcosium/vault/CryptoBars/data
export XDG_RUNTIME_DIR=/run/user/$(id -u)
export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$(id -u)/bus

echo "── $(date '+%F(%a) %H:%M:%S') ──"

echo "[임시 백필 유닛]  (비어 있으면 초기 백필 전부 완료)"
systemctl --user list-units 'cb-*' --all --no-pager --no-legend 2>/dev/null | awk '{print "  "$1,$3,$4}'

echo "[상시 타이머]"
systemctl --user list-timers 'cryptobars-*' --all --no-pager 2>/dev/null | sed -n '2,4p' | sed 's/^/  /'

echo "[한국]"
# 일일 갱신은 타이머 service 가 돌려 출력이 journal 로 간다 —
# backfill_kis.log 만 보면 백필이 끝난 시점에서 "멈춘 것처럼" 보인다.
journalctl --user -u cryptobars-krx.service --no-pager 2>/dev/null \
  | grep -oE '20[0-9]{6}  신규봉 .*' | tail -1 | sed 's/^/  최근 갱신 /'
echo "  DB $(du -h "$V/KRX/bars_ohlc.db" | cut -f1)  journal=$(python3 -c "
import sqlite3;print(sqlite3.connect('file:$V/KRX/bars_ohlc.db?mode=ro',uri=True).execute('pragma journal_mode').fetchone()[0])" 2>/dev/null)"
echo "  백필기 실패 누계 $(grep -cE '^  [0-9]{8} [0-9]{6} 실패' "$V/KRX/backfill_kis.log")건 (done 에 안 남아 갱신이 메운다)"

echo "  토스 백필(2022-11-23~2025-09-07) $(grep -E '^ *[0-9]+/[0-9]+ ' "$V/KRX/backfill_toss.log" 2>/dev/null | tail -1 | sed 's/^ *//')"
echo "  토스 실패 $(grep -c ' 실패 ' "$V/KRX/backfill_toss.log" 2>/dev/null)건 (done 에 안 남아 재실행이 메운다)  DB $(du -h "$V/KRX/bars_toss.db" 2>/dev/null | cut -f1)"

echo "[미국]"
echo "  종목 $(ls "$V/USA/1m" | wc -l)  용량 $(du -sh "$V/USA/1m" | cut -f1)  빈달마커 $(find "$V/USA/1m" -name '*.empty' | wc -l)"
journalctl --user -u cryptobars-usa.service --no-pager 2>/dev/null \
  | grep -oE '완료: .*' | tail -1 | sed 's/^/  최근 갱신 /'

echo "[스냅샷]"
python3 - <<'PY' 2>/dev/null || echo "  상태 파일 없음"
import json, time
d = json.load(open('/home/arcosium/vault/CryptoBars/data/snapshot_state.json'))
gb = sum(v['archive'] for v in d.values()) / 1e9
last = time.strftime('%m/%d %H:%M', time.localtime(max(v['ts'] for v in d.values())))
print(f'  {len(d)}조각 {gb:.1f}GB  최근 {last}')
PY
