"""KIS US zero-delay executions -> observed OHLCV minutes. Quotations only.

Keep Polygon backfill intact. Live minutes have their own DB and provenance.
Master exchange codes come from the official KIS daily symbol masters.
"""
import asyncio, json, math, time, sqlite3, sys, io, zipfile, fcntl
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import websockets
ROOT=Path(__file__).resolve().parent
DB=Path.home()/'vault/CryptoBars/data/USA/realtime.db'


def connect():
    DB.parent.mkdir(parents=True,exist_ok=True)
    d=sqlite3.connect(DB,timeout=10);d.execute('PRAGMA journal_mode=WAL')
    d.execute('CREATE TABLE IF NOT EXISTS bars(symbol TEXT,ts INTEGER,open REAL,high REAL,low REAL,close REAL,volume REAL,source TEXT,PRIMARY KEY(symbol,ts))')
    d.execute('CREATE TABLE IF NOT EXISTS quotes(symbol TEXT PRIMARY KEY,ts REAL,last REAL,source TEXT)')
    d.execute('CREATE TABLE IF NOT EXISTS status(key TEXT PRIMARY KEY,value TEXT)');d.commit();return d


def save_status(d,**value):
    with d:d.execute('INSERT OR REPLACE INTO status VALUES(?,?)',('collector',json.dumps(dict(value,checked_at=time.time()))))


def parse_execution(raw,now=None):
    fields=raw.split('^')
    # Live service adds RSYM before the documented 25 fields.
    if len(fields)==26:fields=fields[1:]
    if len(fields)!=25:raise ValueError('Unexpected KIS execution schema')
    key=fields[0];symbol=key[4:] if key.startswith(('DNAS','DNYS','DAMS','RBAQ','RBAY','RBAA')) else key
    stamp=datetime.strptime(fields[5]+fields[6],'%Y%m%d%H%M%S').replace(tzinfo=ZoneInfo('Asia/Seoul')).timestamp()
    price=float(fields[10]);volume=float(fields[18]);now=time.time() if now is None else now
    if not symbol or not all(math.isfinite(x) for x in [price,volume,stamp]) or price<=0 or volume<0 or not -10<=now-stamp<=120:raise ValueError('Stale or invalid execution')
    return symbol,stamp,price,volume


def store_tick(d,tick):
    symbol,ts,price,volume=tick;bucket=int(ts)//60*60
    with d:
        previous=d.execute('SELECT ts FROM quotes WHERE symbol=?',(symbol,)).fetchone()
        if previous and ts<previous[0]:return False
        d.execute('INSERT INTO bars VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(symbol,ts) DO UPDATE SET high=max(high,excluded.high),low=min(low,excluded.low),close=excluded.close,volume=volume+excluded.volume',(symbol,bucket,price,price,price,price,volume,'KIS HDFSCNT0 observed executions'))
        d.execute('INSERT OR REPLACE INTO quotes VALUES(?,?,?,?)',(symbol,ts,price,'KIS HDFSCNT0 zero-delay US'))
    return True


def subscriptions():
    sys.path.insert(0,str(ROOT.parent/'ArcTrade'))
    from autofolio.research_input import paths
    from autofolio.store import connect as books
    import pyarrow.parquet as pq
    symbols=[]
    with books() as d:
        for r in d.execute("SELECT body FROM paper_books WHERE market='us'"):
            symbols.extend(json.loads(r[0]).get('holdings',{}))
    panel=pq.ParquetFile(paths('us')[0]).read(columns=['symbol'],use_threads=False).column(0).to_pylist()
    symbols=list(dict.fromkeys(s.split('@')[0] for s in symbols+sorted(set(panel))))[:40]
    exchanges={}
    for name in ['nasmst','nysmst','amsmst']:
        r=requests.get('https://new.real.download.dws.co.kr/common/master/'+name+'.cod.zip',timeout=20);r.raise_for_status()
        z=zipfile.ZipFile(io.BytesIO(r.content))
        for line in z.read(z.namelist()[0]).decode('cp949','replace').splitlines():
            f=line.split('\t')
            if len(f)>5:exchanges[f[4]]=f[2]
    local=datetime.now(ZoneInfo('Asia/Seoul'));daytime=10<=local.hour<16
    return [(('R'+{'NAS':'BAQ','NYS':'BAY','AMS':'BAA'}[exchanges[s]]) if daytime else 'D'+exchanges[s])+s for s in symbols if s in exchanges]


async def session(d):
    from equities import read_broker
    broker=read_broker()
    if not broker:raise ValueError('KIS quotation credentials unavailable')
    r=requests.post('https://openapi.koreainvestment.com:9443/oauth2/Approval',json={'grant_type':'client_credentials','appkey':broker.app_key,'secretkey':broker.app_secret},timeout=20);r.raise_for_status();approval=r.json()['approval_key']
    keys=subscriptions();received=0;accepted=0;started=time.time();seen=set()
    async with websockets.connect('ws://ops.koreainvestment.com:21000',ping_interval=None,max_size=2**20) as ws:
        for key in keys:
            await ws.send(json.dumps({'header':{'approval_key':approval,'custtype':'P','tr_type':'1','content-type':'utf-8'},'body':{'input':{'tr_id':'HDFSCNT0','tr_key':key}}}));await asyncio.sleep(.15)
        save_status(d,state='subscribed',symbols=len(keys),accepted=accepted,received=received)
        while time.time()-started<1800:
            try:message=await asyncio.wait_for(ws.recv(),timeout=45)
            except asyncio.TimeoutError:
                save_status(d,state='waiting_ticks',symbols=len(keys),accepted=accepted,received=received);continue
            if message.startswith('0|'):
                head=message.split('|',3)
                if head[1]!='HDFSCNT0':continue
                fields=head[3].split('^')
                count=int(head[2]);width=len(fields)//count if count else 0
                if width not in (25,26) or len(fields)!=width*count:continue
                for i in range(count):
                    raw='^'.join(fields[i*width:(i+1)*width])
                    # Provider may replay the identical tick on reconnect.
                    if raw in seen:continue
                    seen.add(raw)
                    if len(seen)>20000:seen.clear();seen.add(raw)
                    try:
                        if store_tick(d,parse_execution(raw)):received+=1
                    except (ValueError,IndexError) as exc:
                        save_status(d,state='schema_check',field_count=len(fields),batch_count=int(head[2]),error=type(exc).__name__);continue
                if received:save_status(d,state='receiving',symbols=len(keys),accepted=accepted,received=received,last_tick=time.time())
            elif message.startswith('{'):
                data=json.loads(message)
                if data.get('header',{}).get('tr_id')=='PINGPONG':await ws.send(message)
                elif data.get('body',{}).get('rt_cd')=='0':accepted+=1
                else:save_status(d,state='subscription_rejected',symbols=len(keys),accepted=accepted,received=received)


async def main():
    d=connect()
    while True:
        try:await session(d)
        except Exception as exc:save_status(d,state='retrying',error=type(exc).__name__)
        await asyncio.sleep(60)

if __name__=='__main__':
    with (DB.parent/'us-realtime.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);asyncio.run(main())
