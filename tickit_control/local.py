"""Disk-backed local DynamoDB/SQS substitutes for offline race/restart tests."""
import json
from contextlib import closing, contextmanager
import sqlite3
import uuid

from .admission import Record
from .batch import Batch, canonical


class SQLiteStore:
    def __init__(self, filename):
        self.filename = str(filename)
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS records (pk TEXT PRIMARY KEY, revision INTEGER NOT NULL, data TEXT NOT NULL)')

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.filename, timeout=20)) as db:
            with db:
                yield db

    def get(self, key):
        with self.connect() as db:
            row = db.execute('SELECT revision,data FROM records WHERE pk=?', (key,)).fetchone()
        return Record(row[0], json.loads(row[1])) if row else None

    def queued(self, pipeline_id, limit=10):
        with self.connect() as db:
            rows = db.execute('SELECT revision,data FROM records WHERE pk LIKE ?',
                              ('BATCH#'+pipeline_id+'#%',)).fetchall()
        pending = [Record(revision, json.loads(raw)) for revision, raw in rows
                   if json.loads(raw)['state'] == 'QUEUED']
        return sorted(pending, key=lambda r: (r.data['manifest']['load_date'], r.data['manifest']['batch_id']))[:limit]

    def cas(self, changes):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for key, (expected, _) in changes.items():
                row = db.execute('SELECT revision,data FROM records WHERE pk=?', (key,)).fetchone()
                if expected is None:
                    if row is not None:
                        return False
                elif row is None or row[0] != expected.revision or json.loads(row[1]) != expected.data:
                    return False
            for key, (expected, data) in changes.items():
                revision = 1 if expected is None else expected.revision+1
                db.execute('INSERT OR REPLACE INTO records VALUES (?,?,?)', (key, revision, canonical(data)))
        return True


class SQLiteQueue:
    def __init__(self, filename, clock, visibility_seconds=60):
        self.filename, self.clock, self.visibility = str(filename), clock, visibility_seconds
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL, visible REAL NOT NULL, receipt TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS dedup (fingerprint TEXT PRIMARY KEY, sent REAL NOT NULL, message_id INTEGER NOT NULL)')

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.filename, timeout=20)) as db:
            with db:
                yield db

    def send(self, batch, group):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            duplicate = db.execute('SELECT sent,message_id FROM dedup WHERE fingerprint=?', (batch.fingerprint,)).fetchone()
            if duplicate and self.clock()-duplicate[0] < 300:
                return str(duplicate[1])
            cursor = db.execute('INSERT INTO messages(body,visible) VALUES (?,?)', (batch.document, self.clock()))
            seq = cursor.lastrowid
            db.execute('INSERT OR REPLACE INTO dedup VALUES (?,?,?)', (batch.fingerprint, self.clock(), seq))
        return str(seq)

    def receive(self):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT seq,body,visible FROM messages ORDER BY seq LIMIT 1').fetchone()
            if not row or row[2] > self.clock():
                return None
            receipt = uuid.uuid4().hex
            db.execute('UPDATE messages SET visible=?,receipt=? WHERE seq=?', (self.clock()+self.visibility, receipt, row[0]))
        return {'manifest': json.loads(row[1]), 'receipt': receipt}

    def ack(self, receipt):
        with self.connect() as db:
            db.execute('DELETE FROM messages WHERE receipt=?', (receipt,))

    def count(self):
        with self.connect() as db:
            return db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
