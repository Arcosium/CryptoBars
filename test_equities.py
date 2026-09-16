from datetime import datetime
from equities import KST, parse_nxt, open_store, store_nxt


def test_venue_store_updates_without_duplicate_and_ignores_unfinished(tmp_path):
    now=datetime(2026,9,16,10,1,tzinfo=KST)
    row={'stck_bsop_date':'20260916','stck_cntg_hour':'100000','stck_oprc':'100',
         'stck_hgpr':'102','stck_lwpr':'99','stck_prpr':'101','cntg_vol':'12'}
    rows=parse_nxt([row,dict(row,stck_cntg_hour='100100'),dict(row,stck_bsop_date='20260915')],now)
    assert len(rows)==1 and rows[0][-1]=='KIS:NX'
    with open_store(tmp_path/'bars.db') as conn:
        store_nxt(conn,'005930',rows);store_nxt(conn,'005930',rows)
        assert conn.execute('SELECT COUNT(*) FROM bars').fetchone()[0]==1
        assert conn.execute('SELECT close FROM bars').fetchone()[0]==101


def test_bad_ohlcv_rejected():
    row={'stck_bsop_date':'20260916','stck_cntg_hour':'100000','stck_oprc':'100',
         'stck_hgpr':'98','stck_lwpr':'99','stck_prpr':'101','cntg_vol':'12'}
    assert not parse_nxt([row],datetime(2026,9,16,10,1,tzinfo=KST))


def test_nxt_recovers_gap_back_to_last_stored_bar():
    import asyncio
    from equities import collect_nxt_symbol
    calls=[]
    class Broker:
        async def _get_json(self,path,tr,params):
            calls.append(params["FID_INPUT_HOUR_1"])
            hours=["100000","095900"] if len(calls)==1 else ["095800","095700"]
            return {"rt_cd":"0","output2":[{
                "stck_bsop_date":"20260916","stck_cntg_hour":h,"stck_oprc":"100",
                "stck_hgpr":"102","stck_lwpr":"99","stck_prpr":"101","cntg_vol":"12"}
                for h in hours]}
    rows=asyncio.run(collect_nxt_symbol(Broker(),"005930","202609160958",asyncio.Event(),
                                      now=datetime(2026,9,16,10,1,tzinfo=KST)))
    assert calls == ["100100","095859"]
    assert [r[0] for r in rows] == ["202609160957","202609160958","202609160959","202609161000"]
