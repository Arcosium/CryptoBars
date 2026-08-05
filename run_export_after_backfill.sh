#!/bin/bash
# 백필이 끝나기를 기다렸다가 내보내기 + 구글드라이브 전송까지 한 번에.
cd /home/arcosium/projects/CryptoBars
while pgrep -f "backfill.py --start" > /dev/null; do sleep 60; done
echo "=== 백필 종료 확인, 내보내기 시작 $(date '+%F %T') ==="
.venv/bin/python export.py --parquet --upload --remote gdrive:CryptoBars
echo "=== 전체 완료 $(date '+%F %T') ==="
