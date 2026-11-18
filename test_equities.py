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


def test_slow_nxt_pass_does_not_hold_back_krx_crawl(monkeypatch,tmp_path):
    """2026-11-16: a 4-minute KIS NXT pass delayed the 12:59 KRX bars past the paper signal window."""
    import asyncio,sys,types
    import equities
    krx,nxt_calls=[0],[]
    gate=asyncio.Event()
    class Crawler:
        def __init__(self,codes):pass
        def crawl_cycle(self,conn,session):
            krx[0]+=1;return 1
    crawler=types.SimpleNamespace(load_universe=lambda:[('005930','fixture')],Crawler=Crawler,
                                  open_db=lambda:types.SimpleNamespace(close=lambda:None))
    market_time=types.SimpleNamespace(in_crawl_session=lambda now:True)
    monkeypatch.setitem(sys.modules,'market_bars',types.SimpleNamespace(crawler=crawler,market_time=market_time))
    class Broker:
        async def _get_json(self,*args):
            nxt_calls.append(args);await gate.wait();return {"rt_cd":"0","output2":[]}
        async def close(self):pass
    async def tick(stop):await asyncio.sleep(.01)
    monkeypatch.setattr(equities,'DATA',tmp_path)
    monkeypatch.setattr(equities,'read_broker',Broker)
    monkeypatch.setattr(equities,'nxt_session',lambda now:True)
    monkeypatch.setattr(equities,'wait_next_minute',tick,raising=False)
    monkeypatch.setenv('LEADLAG_BARS_DB','unused');monkeypatch.setenv('TIMEFOLIO_UNIVERSE_CSV','unused')
    async def main():
        stop=asyncio.Event();task=asyncio.create_task(equities.run(stop))
        for _ in range(300):
            if krx[0]>=3:break
            await asyncio.sleep(.01)
        crawled,stuck=krx[0],len(nxt_calls)
        stop.set();gate.set();await asyncio.wait_for(task,5)
        return crawled,stuck
    crawled,stuck=asyncio.run(main())
    assert stuck==1 and crawled>=3
