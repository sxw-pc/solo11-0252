"""Check mounted volume snapshots before a replacement instance resumes traffic."""
import re

from .store import one, rows


SCHEMA = '''
CREATE TABLE IF NOT EXISTS volume_requirements(
    service TEXT NOT NULL, pod TEXT NOT NULL, volume TEXT NOT NULL,
    PRIMARY KEY(service,pod,volume)
);
CREATE TABLE IF NOT EXISTS volume_snapshots(
    id TEXT PRIMARY KEY, service TEXT NOT NULL, pod TEXT NOT NULL,
    volume TEXT NOT NULL, source_uid TEXT NOT NULL, checkpoint INTEGER NOT NULL,
    digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS volume_verifications(
    case_id TEXT NOT NULL, uid TEXT NOT NULL, volume TEXT NOT NULL,
    snapshot TEXT NOT NULL, observed_digest TEXT NOT NULL,
    PRIMARY KEY(case_id,uid,volume)
);
'''


def required(db, service, pod):
    return {item['volume'] for item in rows(db, 'SELECT volume FROM volume_requirements WHERE service=? AND pod=?',
                                           (service, pod))}


def ready(db, service, pod, uid, case_id):
    verified = {item['volume'] for item in rows(db, 'SELECT volume FROM volume_verifications WHERE case_id=? AND uid=?',
                                           (case_id, uid))}
    return required(db, service, pod) <= verified


def register(db, x):
    volumes = x['volumes']
    if not volumes or any(not isinstance(name, str) or not name for name in volumes) or len(set(volumes)) != len(volumes):
        return {'decision': 'invalid_volume_requirements'}
    prior = required(db, x['service'], x['pod'])
    if prior:
        return {'decision': 'requirements_recorded' if prior == set(volumes) else 'requirements_conflict'}
    db.executemany('INSERT INTO volume_requirements VALUES(?,?,?)',
                   ((x['service'], x['pod'], volume) for volume in sorted(volumes)))
    return {'decision': 'requirements_recorded'}


def record_snapshot(db, x):
    if x['volume'] not in required(db, x['service'], x['pod']) or not x['source_uid']:
        return {'decision': 'snapshot_unregistered_volume'}
    if type(x['checkpoint']) is not int or x['checkpoint'] < 0 or not re.fullmatch('[0-9a-f]{64}', x['digest']):
        return {'decision': 'invalid_snapshot'}
    prior = one(db, 'SELECT * FROM volume_snapshots WHERE id=?', (x['id'],))
    fields = ('id', 'service', 'pod', 'volume', 'source_uid', 'checkpoint', 'digest')
    if prior:
        return {'decision': 'snapshot_recorded' if all(prior[field] == x[field] for field in fields) else 'snapshot_conflict'}
    db.execute('INSERT INTO volume_snapshots VALUES(?,?,?,?,?,?,?)', tuple(x[field] for field in fields))
    return {'decision': 'snapshot_recorded'}


def verify(db, x):
    scope = one(db, """SELECT d.uid, d.pod, r.service FROM drain_cases d
        JOIN restart_decisions r ON r.id=d.restart_id
        JOIN service_members m ON m.service=r.service AND m.pod=d.pod
        WHERE d.id=? AND d.status='drained' AND r.status='settled' AND m.uid=? AND m.state='recovering'""",
        (x['case'], x['uid']))
    snapshot = one(db, 'SELECT * FROM volume_snapshots WHERE id=?', (x['snapshot'],))
    if (not scope or not snapshot or snapshot['service'] != scope['service'] or snapshot['pod'] != scope['pod']
            or snapshot['source_uid'] != scope['uid'] or snapshot['volume'] != x['volume']):
        return {'decision': 'wrong_volume_recovery'}
    if snapshot['digest'] != x['observed_digest']:
        return {'decision': 'volume_digest_mismatch'}
    key = (x['case'], x['uid'], x['volume'])
    prior = one(db, 'SELECT * FROM volume_verifications WHERE case_id=? AND uid=? AND volume=?', key)
    if prior:
        return {'decision': 'volume_verified' if prior['snapshot'] == x['snapshot'] else 'verification_conflict'}
    db.execute('INSERT INTO volume_verifications VALUES(?,?,?,?,?)', (*key, x['snapshot'], x['observed_digest']))
    return {'decision': 'volume_verified'}


def handle(db, x):
    if x['op'] == 'requirements-register':
        return register(db, x)
    if x['op'] == 'snapshot-record':
        return record_snapshot(db, x)
    if x['op'] == 'snapshot-verify':
        return verify(db, x)
    return {'decision': 'unknown_operation'}
