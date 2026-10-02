import json, os, sys

import backfill_polygon as bp
import refresh_polygon as rp


def test_recheck_refetches_month_written_before_it_ended_only_once(tmp_path, monkeypatch):
    monkeypatch.setattr(bp, 'OUT', str(tmp_path))
    calls = []
    monkeypatch.setattr(bp, 'fetch', lambda s, k, t, ym: calls.append(ym) or [{'t': 1, 'o': 1, 'h': 1, 'l': 1, 'c': 1, 'v': 1}])
    d = tmp_path / 'AAA'
    d.mkdir()
    for ym, after in (('2020-01', True), ('2020-02', False)):   # 1월=월말 뒤에 받음, 2월=월말 전에 받음(꼬리 빠짐)
        p = d / f'{ym}.parquet'
        p.write_bytes(b'x')
        ts = bp.settled_at(ym) + (3600 if after else -3600)
        os.utime(p, (ts, ts))
    ms = ['2020-01', '2020-02', '2020-03']
    bp.one('k', 'AAA', ms, 1, recheck=True)
    assert calls == ['2020-02', '2020-03']          # 이번 달(refresh) + 꼬리 빠진 2월
    calls.clear()
    bp.one('k', 'AAA', ms, 1, recheck=True)
    assert calls == ['2020-03']                     # 2월은 방금 다시 써서 더는 안 받는다
    calls.clear()
    bp.one('k', 'AAA', ms, 1)                       # 백필 경로(recheck 없음)는 예전 그대로
    assert calls == ['2020-03']


def test_cursor_advances_by_finished_prefix_when_budget_runs_out(tmp_path, monkeypatch):
    uni = tmp_path / 'universe.txt'
    uni.write_text('\n'.join(['T0'] + [f'R{i}' for i in range(6)]) + '\n')
    monkeypatch.setattr(bp, 'UNIVERSE', str(uni))
    monkeypatch.setattr(rp, 'STATE', str(tmp_path / 'state.json'))
    monkeypatch.setattr(bp, 'api_key', lambda: 'k')
    monkeypatch.setattr(rp, 'top_by_dollar_volume', lambda key, n: (['T0'], '2026-10-01'))
    clock = [0.0]
    monkeypatch.setattr(rp.time, 'time', lambda: clock[0])

    def fake_one(key, t, ms, refresh, recheck=False):
        clock[0] += 1000                            # 종목당 1000초 — 예산 3600초면 4종목째 이후 시간 초과
        if t == 'R1':
            raise RuntimeError('boom')              # 실패해도 커서는 넘어간다
        return t, 1, 10
    monkeypatch.setattr(bp, 'one', fake_one)
    monkeypatch.setattr(sys, 'argv', ['refresh_polygon.py', '--top', '1', '--rotate', '5',
                                      '--min-interval', '0', '--budget-hours', '1'])
    rp.main()
    st = json.load(open(tmp_path / 'state.json'))
    assert st['cursor'] == 3 and st['rotate'] == 3 and st['fail'] == 1   # T0·R0·R1·R2 처리, R3·R4 는 다음 실행
