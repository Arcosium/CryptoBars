#!/bin/bash
# 350종목 백필이 끝나면 곧바로 전종목(2,565) 백필을 이어 돌린다.
# done 장부 덕에 이미 받은 350종목은 건너뛰므로 실제로는 나머지 2,215종목만 받는다.
L=/home/arcosium/vault/CryptoBars/data/KRX/backfill_kis.log
until ! pgrep -f 'backfill_kis\.py$' >/dev/null 2>&1; do sleep 60; done
echo "=== 350종목 백필 종료 $(date '+%F %T') — 전종목 백필 시작 ===" >> "$L"
cd /home/arcosium/projects/CryptoBars
python3 backfill_kis_all.py >> "$L" 2>&1
echo "=== 전종목(2,565) 백필 종료 $(date '+%F %T') ===" >> "$L"
