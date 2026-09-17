#!/bin/bash
# 1차(상위500) 백필이 끝나면 곧바로 확장 유니버스(1,313종목) 5년 백필을 이어 돌린다.
# 이미 받은 종목·월은 파일이 있어 건너뛰므로 실제로는 추가 813종목만 받는다.
L=/home/arcosium/vault/CryptoBars/data/USA/backfill_polygon.log
until ! pgrep -f 'backfill_polygon.py' >/dev/null 2>&1; do sleep 60; done
echo "=== 1차(상위500) 종료 $(date '+%F %T') — 확장 백필 시작 ===" >> "$L"
cd /home/arcosium/projects/CryptoBars
python3 backfill_polygon.py --years 5 --workers 6 >> "$L" 2>&1
echo "=== 확장(1,313종목) 종료 $(date '+%F %T') ===" >> "$L"
