"""Consistent live-market snapshots supplement the existing weekly history backup.

Read SQLite through its online backup API. Never checkpoint/stop collectors.
Drive metadata MD5 and size must match before recording a successful upload.
"""
import fcntl,hashlib,json,os,shutil,sqlite3,subprocess,sys,tempfile,time
from pathlib import Path
from contextlib import closing
DATA=Path.home()/'vault/CryptoBars/data'
STATE=DATA/'live_snapshot_state.json'


def backup_sqlite(source,destination):
    destination.parent.mkdir(parents=True,exist_ok=True)
    with closing(sqlite3.connect(source.resolve().as_uri()+'?mode=ro',uri=True,timeout=5)) as src, closing(sqlite3.connect(destination)) as dst:
        src.backup(dst,pages=256,sleep=.05)
        if dst.execute('PRAGMA quick_check').fetchone()!=('ok',):raise ValueError('Snapshot SQLite integrity failed')


def main():
    from dotenv import load_dotenv
    load_dotenv(Path.home()/'projects/.env')
    sys.path.insert(0,str(Path.home()/'projects/ArcAI.ve/clients'));import drive_mcp as drive
    with (DATA/'live-snapshot.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        with tempfile.TemporaryDirectory(prefix='live-snapshot-',dir=DATA.parent) as tmp:
            root=Path(tmp);content=root/'content';content.mkdir();files=[]
            for name in ['KRX/bars.db','NXT/bars.db','USA/realtime.db']:
                source=DATA/name
                if source.exists():backup_sqlite(source,content/name);files.append(name)
            # Flushed crypto buffers are immutable. Weekly history backup covers compaction.
            for source in (DATA/'bars').glob('date=*/*.parquet'):
                name=source.relative_to(DATA);dest=content/name;dest.parent.mkdir(parents=True,exist_ok=True)
                try:shutil.copyfile(source,dest);files.append(str(name))
                except FileNotFoundError:dest.unlink(missing_ok=True) # Compacted concurrently; weekly history retains it.
            manifest=dict(captured_at=time.time(),files=files,crypto_scope='flushed buffers; consolidated history uses weekly snapshot')
            (content/'manifest.json').write_text(json.dumps(manifest))
            archive=root/'live-market.tar.zst'
            subprocess.run(['tar','--use-compress-program=zstd -1 -T2','-cf',str(archive),'-C',str(content),'.'],check=True)
            with archive.open('rb') as stream:digest=hashlib.file_digest(stream,'md5').hexdigest()
            name='live-market-'+time.strftime('%Y-%m-%d',time.gmtime())+'.tar.zst'
            drive.upload_file(str(archive),name=name,folder='market-data-snapshot')
            parent=drive._find_or_create_folder('market-data-snapshot',None);identity=drive._find_file_in(name,parent)
            remote=drive._req('GET',drive.DRIVE_API+'/files/'+identity,params={'fields':'id,name,size,md5Checksum,modifiedTime'})
            if remote.get('md5Checksum')!=digest or int(remote['size'])!=archive.stat().st_size:raise ValueError('Drive checksum verification failed')
            manifest.update(verified_at=time.time(),archive_bytes=archive.stat().st_size,remote=remote,md5=digest)
            temp=STATE.with_suffix('.tmp');temp.write_text(json.dumps(manifest));temp.replace(STATE)
            print(json.dumps({k:manifest[k] for k in ['captured_at','verified_at','archive_bytes','remote']}),flush=True)

if __name__=='__main__':main()
