#!/bin/bash
# 한국 전종목 백필이 끝나면 검증·스냅샷까지 자동으로 마무리하고, 일일 갱신 타이머에 넘긴다.
# setsid 로 띄우므로 대화 세션이 끊겨도 끝까지 간다.
set -u
D=/home/arcosium/vault/CryptoBars/data/KRX
LOG=$D/finalize.log
exec >>"$LOG" 2>&1
echo "=== finalize_krx 대기 시작 $(date '+%F %T') ==="

# 1) 전종목 백필 종료 대기 (chain_krx_all.sh 가 돌리는 backfill_kis_all.py)
until ! pgrep -f 'backfill_kis' >/dev/null 2>&1; do sleep 300; done
echo "=== 백필 종료 $(date '+%F %T') ==="
grep -E "^=== " "$D/backfill_kis.log" | tail -2

# 2) 검증 리포트
python3 - <<'PY'
import sqlite3, json, time, os
D='/home/arcosium/vault/CryptoBars/data/KRX'
c=sqlite3.connect(f'file:{D}/bars_ohlc.db?mode=ro',uri=True)
n=c.execute('select count(*) from bars').fetchone()[0]
days=c.execute('select count(distinct day) from done').fetchone()[0]
codes=c.execute('select count(distinct code) from done').fetchone()[0]
full=c.execute('select count(*) from (select day from done group by day having count(distinct code)>=2500)').fetchone()[0]
rng=c.execute('select min(substr(ts,1,8)),max(substr(ts,1,8)) from bars').fetchone()
# 하루 381봉(09:00~15:30)이 정상 — 표본 검사
odd=c.execute('''select count(*) from (
  select code, substr(ts,1,8) d, count(*) n from bars group by code,d having n>400)''').fetchone()[0]
rep={'ts':time.strftime('%F %T'),'bars':n,'days':days,'codes':codes,
     'full_days':full,'range':rng,'db_gb':round(os.path.getsize(f'{D}/bars_ohlc.db')/1e9,2),
     'over_400_bars_days':odd}
json.dump(rep, open(f'{D}/verify_report.json','w'), ensure_ascii=False, indent=1)
print(f"{n:,}봉  거래일 {days}  종목 {codes:,}  전종목완료일 {full}  {rep['db_gb']}GB  구간 {rng[0]}~{rng[1]}")
PY

# 3) 다른 스냅샷이 돌고 있으면 기다렸다가 업로드
until ! pgrep -f 'snapshot_drive\.py' >/dev/null 2>&1; do sleep 120; done
echo "=== 스냅샷 시작 $(date '+%F %T') ==="
cd /home/arcosium/projects/CryptoBars && python3 snapshot_drive.py --source krx
echo "=== finalize_krx 완료 $(date '+%F %T') ==="
echo "이후 일일 갱신은 cryptobars-krx.timer(21:00)가 맡는다 — ExecCondition 이 백필 중에는 건너뛴다."
