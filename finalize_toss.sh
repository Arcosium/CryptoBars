#!/bin/bash
# 토스 1분봉 백필(cb-backfill-toss)이 끝나면 보충 패스와 검증까지 마친다.
# user 유닛(cb-finalize-toss)으로 띄우므로 대화 세션·arcai-ve 재시작과 무관하게 끝까지 간다.
# 스냅샷은 하지 않는다 — snapshot_drive.py 는 아직 bars_toss.db 를 모른다(사장 결정 후 추가).
set -u
export XDG_RUNTIME_DIR=/run/user/$(id -u)
D=/home/arcosium/vault/CryptoBars/data/KRX
LOG=$D/finalize_toss.log
exec >>"$LOG" 2>&1
echo "=== finalize_toss 대기 시작 $(date '+%F %T') ==="

# 1) 본 백필 종료 대기 — pgrep 대신 유닛 상태로 본다(pgrep -f 는 자기 명령줄에 걸린다)
while systemctl --user is-active -q cb-backfill-toss; do sleep 300; done
echo "=== 백필 종료 $(date '+%F %T')  $(systemctl --user show cb-backfill-toss -p Result -p ExecMainStatus | tr '\n' ' ') ==="

# 2) 보충 패스 — 실패 종목(9/30 041830: 응답이 끊겨 JSON 잘림)은 done 에 없으니 한 번 더 돌리면 그것만 받는다.
#    지금 코드는 잘린 응답도 재시도한다(본 백필 프로세스는 수정 전 코드로 돌았다).
cd /home/arcosium/projects/CryptoBars
echo "=== 보충 패스 $(date '+%F %T') ===" | tee -a "$D/backfill_toss.log"
/usr/bin/python3 -u backfill_toss.py --workers 4 >> "$D/backfill_toss.log" 2>&1
echo "보충 패스 종료 rc=$? $(date '+%F %T')  실패 $(sed -n '/=== 보충 패스/,$p' "$D/backfill_toss.log" | grep -c ' 실패 ')건"

# 3) 검증 — 수집이 멈춘 뒤라 count(*) 를 써도 된다
/usr/bin/python3 - <<'PY'
import csv, json, os, sqlite3, time
D = '/home/arcosium/vault/CryptoBars/data/KRX'
c = sqlite3.connect(f'file:{D}/bars_toss.db?mode=ro', uri=True)
n, lo, hi, codes = c.execute('select count(*), min(ts), max(ts), count(distinct code) from bars').fetchone()
done, notfound, s = c.execute("select count(*), sum(status='notfound'), "
                              "sum(case when status='notfound' then 0 else cast(status as int) end) from done").fetchone()
universe = {r['code'] for r in csv.DictReader(open(f'{D}/universe_all.csv', newline=''))}
missing = sorted(universe - {r[0] for r in c.execute('select code from done')})
rep = {'ts': time.strftime('%F %T'), 'bars': n, 'done_sum': s, 'bars_eq_done_sum': n == s,
       'codes_with_bars': codes, 'done': done, 'notfound': notfound,
       'notfound_codes': [r[0] for r in c.execute("select code from done where status='notfound'")],
       'missing_codes': missing, 'range': [lo, hi],
       'quick_check': c.execute('pragma quick_check').fetchone()[0],
       'db_gb': round(os.path.getsize(f'{D}/bars_toss.db') / 1e9, 2)}
json.dump(rep, open(f'{D}/verify_toss.json', 'w'), ensure_ascii=False, indent=1)
print(f"{n:,}봉 (done 합계 {s:,}, 일치 {n == s})  종목 {codes:,}  404 {notfound}  미완료 {len(missing)} {missing[:10]}  "
      f"{lo}~{hi}  quick_check {rep['quick_check']}  {rep['db_gb']}GB")
PY
echo "=== finalize_toss 완료 $(date '+%F %T') ==="
