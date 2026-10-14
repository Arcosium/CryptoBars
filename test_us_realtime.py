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
