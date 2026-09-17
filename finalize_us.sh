#!/bin/bash
# 미국 전체 백필이 끝나면 정리·검증·스냅샷까지 자동으로 마무리한다.
# setsid 로 띄우므로 대화 세션이 끊겨도 끝까지 간다.
set -u
D=/home/arcosium/vault/CryptoBars/data/USA
LOG=$D/finalize.log
exec >>"$LOG" 2>&1
echo "=== finalize_us 대기 시작 $(date '+%F %T') ==="

# 1) 백필 종료 대기
until ! pgrep -f 'backfill_polygon\.py' >/dev/null 2>&1; do sleep 120; done
echo "=== 백필 종료 $(date '+%F %T') ==="
tail -2 "$D/backfill_full.log"

# 2) 데이터가 하나도 없는 종목 폴더 제거 (거래 기록이 없는 심볼)
EMPTY=$(find "$D/1m" -mindepth 1 -maxdepth 1 -type d -empty | wc -l)
find "$D/1m" -mindepth 1 -maxdepth 1 -type d -empty -delete
echo "빈 폴더 제거: $EMPTY 개"

# 3) 검증 리포트
python3 - <<'PY'
import os, json, collections
D='/home/arcosium/vault/CryptoBars/data/USA'
root=os.path.join(D,'1m')
# .empty 마커(빈 달 기억용)는 세지 않는다 — 파케이가 하나라도 있는 종목만 집계
cnt={t:sum(f.endswith('.parquet') for f in os.listdir(os.path.join(root,t))) for t in sorted(os.listdir(root))}
cnt={t:n for t,n in cnt.items() if n}
tick=sorted(cnt)
size=sum(os.path.getsize(os.path.join(r,f)) for r,_,fs in os.walk(root) for f in fs)
dist=collections.Counter(cnt.values())
pit=json.load(open(os.path.join(D,'universe_pit.json')))
have=set(tick)
cov={d: round(len({x['ticker'] for x in v} & have)/len(v)*100,1) for d,v in sorted(pit.items())}
rep={'ts':__import__('time').strftime('%F %T'),'tickers':len(tick),
     'files':sum(cnt.values()),'size_gb':round(size/1e9,2),
     'full_61m':dist.get(61,0),'pit_coverage':cov,
     'worst_coverage':min(cov.values()) if cov else None}
json.dump(rep, open(os.path.join(D,'verify_report.json'),'w'), ensure_ascii=False, indent=1)
print(f"종목 {rep['tickers']:,}  파일 {rep['files']:,}  {rep['size_gb']}GB  "
      f"61개월완전 {rep['full_61m']:,}  시점커버 최저 {rep['worst_coverage']}%")
PY

# 4) 다른 스냅샷이 돌고 있으면 기다렸다가 연도별 업로드
until ! pgrep -f 'snapshot_drive\.py' >/dev/null 2>&1; do sleep 120; done
echo "=== 스냅샷 시작 $(date '+%F %T') ==="
cd /home/arcosium/projects/CryptoBars && python3 snapshot_drive.py --source usa
echo "=== finalize_us 완료 $(date '+%F %T') ==="
