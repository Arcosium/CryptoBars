#!/bin/bash
# 수집 상태 한 눈에 — 무거운 count(*) 를 쓰지 않는다.
# journal_mode=delete 인 DB 에 count(*) 를 걸면 읽는 동안 쓰기가 막혀 수집이 죽는다
# (2026-09-21 실사고: 상태 확인 쿼리가 백필을 OperationalError 로 죽였다).
# 대신 로그 마지막 줄의 '누적' 과 파일 크기를 읽는다. 정확한 행 수가 필요하면
# 수집이 멈춘 뒤 pyarrow/ sqlite 로 따로 센다.
V=/home/arcosium/vault/CryptoBars/data
export XDG_RUNTIME_DIR=/run/user/$(id -u) DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$(id -u)/bus
echo "── $(date '+%F(%a) %H:%M:%S') ──"
echo "[임시 백필 유닛]"; systemctl --user list-units 'cb-*' --all --no-pager --no-legend 2>/dev/null | awk '{print "  "$1,$3,$4}'
echo "[상시 타이머]"; systemctl --user list-timers 'cryptobars-*' --all --no-pager 2>/dev/null | sed -n '2,4p' | sed 's/^/  /'
echo "[한국]"
grep -E "^20" "$V/KRX/backfill_kis.log" | tail -1 | sed 's/^/  /'
echo "  DB $(du -h "$V/KRX/bars_ohlc.db" | cut -f1)  로그최종 $(stat -c '%y' "$V/KRX/backfill_kis.log" | cut -c12-19)"
echo "  실패 누계 $(grep -cE '^  [0-9]{8} [0-9]{6} 실패' "$V/KRX/backfill_kis.log")건 (done 에 안 남아 다음 실행이 메움)"
echo "[미국]"
echo "  종목 $(ls "$V/USA/1m" | wc -l)  용량 $(du -sh "$V/USA/1m" | cut -f1)  마커 $(find "$V/USA/1m" -name '*.empty' | wc -l)"
journalctl --user -u cryptobars-usa.service --no-pager 2>/dev/null | grep '완료:' | tail -1 | sed 's/^.*python3\[[0-9]*\]: /  최근 갱신 /'
echo "[스냅샷]"; python3 -c "
import json,time
try: d=json.load(open('$V/snapshot_state.json'))
except Exception: print('  없음'); raise SystemExit
print(f'  {len(d)}조각 {sum(v[\"archive\"] for v in d.values())/1e9:.1f}GB  최근 {time.strftime(\"%m/%d %H:%M\", time.localtime(max(v[\"ts\"] for v in d.values())))}')"
