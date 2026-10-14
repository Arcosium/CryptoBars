import sqlite3
from live_snapshot import backup_sqlite

def test_online_snapshot_includes_uncheckpointed_wal(tmp_path):
    source=tmp_path/'live.db';dest=tmp_path/'copy.db'
    with sqlite3.connect(source) as db:
        db.execute('PRAGMA journal_mode=WAL');db.execute('CREATE TABLE bars(ts,close)')
        db.execute('INSERT INTO bars VALUES(1,123)');db.commit()
        backup_sqlite(source,dest)
        with sqlite3.connect(dest) as copy:assert copy.execute('SELECT * FROM bars').fetchall()==[(1,123)]
        db.execute('INSERT INTO bars VALUES(2,124)');db.commit()
        with sqlite3.connect(dest) as copy:assert copy.execute('SELECT count(*) FROM bars').fetchone()==(1,)
