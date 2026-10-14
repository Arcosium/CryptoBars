import sqlite3
import pytest
import us_realtime as live
from datetime import datetime
from zoneinfo import ZoneInfo


def test_live_26_fields_and_documented_25_and_delayed_rejection():
    now=datetime(2026,10,14,14,0,tzinfo=ZoneInfo('Asia/Seoul')).timestamp()
    fields=['META','4','20261014','20261014','010000','20261014','140000','760','765','759','762','2','2','.1','761','763','1','1','12','1000','2000','1','1','100','1']
    a=live.parse_execution('^'.join(fields),now)
    b=live.parse_execution('RBAQMETA^'+'^'.join(fields),now)
    assert a==b==('META',now,762.,12.)
    with pytest.raises(ValueError):live.parse_execution('^'.join(fields),now+121)
    fields[10]='nan'
    with pytest.raises(ValueError):live.parse_execution('^'.join(fields),now)


def test_ohlcv_and_older_ticks_do_not_overwrite(tmp_path,monkeypatch):
    monkeypatch.setattr(live,'DB',tmp_path/'quotes.db');d=live.connect()
    assert live.store_tick(d,('A',120.,10.,2.))
    assert live.store_tick(d,('A',130.,12.,3.))
    assert not live.store_tick(d,('A',125.,1.,9.))
    assert d.execute('SELECT open,high,low,close,volume FROM bars').fetchone()==(10.,12.,10.,12.,5.)


def test_operating_minute_universe_precedes_comparison_and_alphabetical_symbols():
    import json
    d=sqlite3.connect(':memory:')
    d.executescript('CREATE TABLE paper_books(user_id,market,body);CREATE TABLE minute_validation(strategy_id,body);CREATE TABLE strategy_assignments(user_id,target,strategy_id);')
    d.execute('INSERT INTO paper_books VALUES(1,?,?)',('us',json.dumps({'holdings':{'NVDA@0':{}}})))
    d.execute('INSERT INTO paper_books VALUES(-1,?,?)',('us',json.dumps({'holdings':{'OTHER':{}}})))
    d.execute("INSERT INTO strategy_assignments VALUES(1,'us-paper','active')")
    d.execute('INSERT INTO minute_validation VALUES(?,?)',('active',json.dumps({'symbols':['MSFT@0','META@0','NVDA@0']})))
    result=live.priority_symbols(d,['A'+str(i) for i in range(50)])
    assert result[:4]==['NVDA','MSFT','META','OTHER'] and len(result)==40


@pytest.mark.parametrize('stamp,daytime,boundary',[
    ('2026-10-14T09:59:59+09:00',False,'2026-10-14T10:00:00+09:00'),
    ('2026-10-14T10:00:00+09:00',True,'2026-10-14T17:00:00+09:00'),
    ('2026-10-14T16:00:00+09:00',True,'2026-10-14T17:00:00+09:00'),
    ('2026-10-14T16:59:59+09:00',True,'2026-10-14T17:00:00+09:00'),
    ('2026-10-14T17:00:00+09:00',False,'2026-10-15T10:00:00+09:00'),
    ('2026-10-14T18:00:00+09:00',False,'2026-10-15T10:00:00+09:00'),
    ('2026-12-14T17:00:00+09:00',True,'2026-12-14T18:00:00+09:00'),
    ('2026-12-14T17:59:59+09:00',True,'2026-12-14T18:00:00+09:00'),
    ('2026-12-14T18:00:00+09:00',False,'2026-12-15T10:00:00+09:00'),
    ('2026-03-06T17:30:00+09:00',True,'2026-03-06T18:00:00+09:00'),
    ('2026-03-09T16:30:00+09:00',True,'2026-03-09T17:00:00+09:00'),
    ('2026-10-30T16:30:00+09:00',True,'2026-10-30T17:00:00+09:00'),
    ('2026-11-02T17:30:00+09:00',True,'2026-11-02T18:00:00+09:00'),
    ('2026-10-14T07:59:59+00:00',True,'2026-10-14T17:00:00+09:00'),
])
def test_subscription_window_uses_us_dst(stamp,daytime,boundary):
    actual,deadline=live.subscription_window(datetime.fromisoformat(stamp))
    assert actual is daytime
    assert deadline.isoformat()==boundary


def test_session_leaves_at_market_boundary_without_waiting_30_minutes(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    start=datetime.fromisoformat('2026-10-14T16:59:59+09:00')
    clock=[start.timestamp()];timeouts=[];selected=[];closed=[]
    class ClockDateTime(datetime):
        @classmethod
        def now(cls,tz=None):return start.astimezone(tz)
    class Socket:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):closed.append(True)
        async def send(self,data):pass
        async def recv(self):raise AssertionError('wait_for is replaced')
    async def wait_for(coro,timeout):
        coro.close();timeouts.append(timeout);clock[0]+=timeout
        raise asyncio.TimeoutError
    async def sleep(delay):pass
    monkeypatch.setattr(live,'datetime',ClockDateTime)
    monkeypatch.setattr(live.time,'time',lambda:clock[0])
    monkeypatch.setitem(live.sys.modules,'equities',SimpleNamespace(read_broker=lambda:SimpleNamespace(app_key='test',app_secret='test')))
    monkeypatch.setattr(live.requests,'post',lambda *a,**k:SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'approval_key':'test'}))
    monkeypatch.setattr(live,'subscriptions',lambda now:(selected.append(live.subscription_window(now)[0]) or ['RBAQA']))
    monkeypatch.setattr(live.websockets,'connect',lambda *a,**k:Socket())
    monkeypatch.setattr(live.asyncio,'wait_for',wait_for)
    monkeypatch.setattr(live.asyncio,'sleep',sleep)
    monkeypatch.setattr(live,'save_status',lambda *a,**k:None)
    asyncio.run(live.session(None))
    assert selected==[True] and timeouts==[1.0] and closed==[True]


def test_successful_session_reconnects_without_error_backoff(monkeypatch):
    import asyncio
    calls=[]
    async def session(d):
        calls.append(d)
        if len(calls)==2:raise asyncio.CancelledError
    async def sleep(delay):raise AssertionError('Normal market transition must not back off')
    monkeypatch.setattr(live,'connect',lambda:'test-store')
    monkeypatch.setattr(live,'session',session)
    monkeypatch.setattr(live.asyncio,'sleep',sleep)
    with pytest.raises(asyncio.CancelledError):asyncio.run(live.main())
    assert calls==['test-store','test-store']
