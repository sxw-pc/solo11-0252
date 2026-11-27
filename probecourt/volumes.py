"""Check mounted volume snapshots before a replacement instance resumes traffic."""
import itertools
import json
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
    digest TEXT NOT NULL, transactions TEXT, withdrawn INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS volume_verifications(
    case_id TEXT NOT NULL, uid TEXT NOT NULL, volume TEXT NOT NULL,
    snapshot TEXT NOT NULL, observed_digest TEXT NOT NULL,
    PRIMARY KEY(case_id,uid,volume)
);
CREATE TABLE IF NOT EXISTS transaction_manifest(
    service TEXT NOT NULL, pod TEXT NOT NULL, txn TEXT NOT NULL,
    acked INTEGER NOT NULL,
    PRIMARY KEY(service,pod,txn)
);
CREATE TABLE IF NOT EXISTS transaction_manifest_volumes(
    service TEXT NOT NULL, pod TEXT NOT NULL, txn TEXT NOT NULL,
    volume TEXT NOT NULL,
    PRIMARY KEY(service,pod,txn,volume)
);
CREATE TABLE IF NOT EXISTS recovery_plans(
    id TEXT PRIMARY KEY, case_id TEXT NOT NULL, service TEXT NOT NULL, pod TEXT NOT NULL,
    source_uid TEXT NOT NULL, uid TEXT NOT NULL,
    selections TEXT NOT NULL, preserved TEXT NOT NULL, status TEXT NOT NULL
);
'''

MAX_VOLUMES = 4
MAX_SNAPSHOTS_PER_VOLUME = 4


def required(db, service, pod):
    return {item['volume'] for item in rows(db, 'SELECT volume FROM volume_requirements WHERE service=? AND pod=?',
                                           (service, pod))}


def manifest_txns(db, service, pod):
    txns = {item['txn']: {'acked': bool(item['acked']), 'volumes': set()}
            for item in rows(db, 'SELECT txn, acked FROM transaction_manifest WHERE service=? AND pod=?',
                             (service, pod))}
    for item in rows(db, 'SELECT txn, volume FROM transaction_manifest_volumes WHERE service=? AND pod=?',
                     (service, pod)):
        txns[item['txn']]['volumes'].add(item['volume'])
    return txns


def ready(db, service, pod, uid, case_id, plan_id=None):
    """Return None when traffic may resume, else the decision string explaining why not."""
    if manifest_txns(db, service, pod):
        if not plan_id:
            return 'recovery_plan_required'
        plan = one(db, "SELECT * FROM recovery_plans WHERE id=? AND case_id=? AND uid=? AND status='approved'",
                   (plan_id, case_id, uid))
        if not plan:
            return 'recovery_plan_not_approved'
        selections = json.loads(plan['selections'])
        verified = {item['volume']: item['snapshot'] for item in rows(
            db, 'SELECT volume, snapshot FROM volume_verifications WHERE case_id=? AND uid=?', (case_id, uid))}
        if all(verified.get(volume) == snapshot for volume, snapshot in selections.items()):
            return None
        return 'volumes_not_verified'
    verified = {item['volume'] for item in rows(db, 'SELECT volume FROM volume_verifications WHERE case_id=? AND uid=?',
                                                (case_id, uid))}
    return None if required(db, service, pod) <= verified else 'volumes_not_verified'


def register(db, x):
    volumes = x['volumes']
    if (not volumes or any(not isinstance(name, str) or not name for name in volumes)
            or len(set(volumes)) != len(volumes) or len(volumes) > MAX_VOLUMES):
        return {'decision': 'invalid_volume_requirements'}
    prior = required(db, x['service'], x['pod'])
    if prior:
        return {'decision': 'requirements_recorded' if prior == set(volumes) else 'requirements_conflict'}
    db.executemany('INSERT INTO volume_requirements VALUES(?,?,?)',
                   ((x['service'], x['pod'], volume) for volume in sorted(volumes)))
    return {'decision': 'requirements_recorded'}


def record_manifest(db, x):
    txns = x.get('transactions')
    req = required(db, x['service'], x['pod'])
    parsed = {}
    if isinstance(txns, list) and txns:
        for item in txns:
            volumes = item.get('volumes') if isinstance(item, dict) else None
            if (not isinstance(item, dict) or not isinstance(item.get('id'), str) or not item['id'] or item['id'] in parsed
                    or not isinstance(volumes, list) or not volumes
                    or any(not isinstance(name, str) or not name for name in volumes)
                    or len(set(volumes)) != len(volumes) or not set(volumes) <= req
                    or type(item.get('acked')) is not bool):
                return {'decision': 'invalid_manifest'}
            parsed[item['id']] = (set(volumes), item['acked'])
    if not parsed:
        return {'decision': 'invalid_manifest'}
    existing = manifest_txns(db, x['service'], x['pod'])
    if existing:
        same = set(existing) == set(parsed) and all(
            existing[txn]['volumes'] == volumes and existing[txn]['acked'] == acked
            for txn, (volumes, acked) in parsed.items())
        return {'decision': 'manifest_recorded' if same else 'manifest_conflict'}
    for txn, (volumes, acked) in parsed.items():
        db.execute('INSERT INTO transaction_manifest VALUES(?,?,?,?)', (x['service'], x['pod'], txn, int(acked)))
        db.executemany('INSERT INTO transaction_manifest_volumes VALUES(?,?,?,?)',
                       ((x['service'], x['pod'], txn, volume) for volume in sorted(volumes)))
    return {'decision': 'manifest_recorded'}


def record_snapshot(db, x):
    if x['volume'] not in required(db, x['service'], x['pod']) or not x['source_uid']:
        return {'decision': 'snapshot_unregistered_volume'}
    if type(x['checkpoint']) is not int or x['checkpoint'] < 0 or not re.fullmatch('[0-9a-f]{64}', x['digest']):
        return {'decision': 'invalid_snapshot'}
    txns = x.get('transactions')
    if txns is not None:
        if (not isinstance(txns, list) or any(not isinstance(txn, str) or not txn for txn in txns)
                or len(set(txns)) != len(txns)):
            return {'decision': 'invalid_snapshot'}
        manifest = manifest_txns(db, x['service'], x['pod'])
        if any(txn not in manifest or x['volume'] not in manifest[txn]['volumes'] for txn in txns):
            return {'decision': 'snapshot_unknown_transaction'}
    prior = one(db, 'SELECT * FROM volume_snapshots WHERE id=?', (x['id'],))
    fields = ('id', 'service', 'pod', 'volume', 'source_uid', 'checkpoint', 'digest')
    if prior:
        prior_txns = set(json.loads(prior['transactions'])) if prior['transactions'] is not None else None
        same = all(prior[field] == x[field] for field in fields) and prior_txns == (set(txns) if txns is not None else None)
        return {'decision': 'snapshot_recorded' if same else 'snapshot_conflict'}
    mounted = one(db, '''SELECT COUNT(*) n FROM volume_snapshots
                         WHERE service=? AND pod=? AND volume=? AND source_uid=? AND withdrawn=0''',
                  (x['service'], x['pod'], x['volume'], x['source_uid']))['n']
    if mounted >= MAX_SNAPSHOTS_PER_VOLUME:
        return {'decision': 'snapshot_limit_exceeded'}
    db.execute('''INSERT INTO volume_snapshots(id,service,pod,volume,source_uid,checkpoint,digest,transactions,withdrawn)
                  VALUES(?,?,?,?,?,?,?,?,0)''',
               tuple(x[field] for field in fields) + (json.dumps(sorted(txns)) if txns is not None else None,))
    return {'decision': 'snapshot_recorded'}


def withdraw_snapshot(db, x):
    snapshot = one(db, 'SELECT * FROM volume_snapshots WHERE id=?', (x['id'],))
    if not snapshot:
        return {'decision': 'unknown_snapshot'}
    if not snapshot['withdrawn']:
        db.execute('UPDATE volume_snapshots SET withdrawn=1 WHERE id=?', (x['id'],))
    return {'decision': 'snapshot_withdrawn'}


def recovery_scope(db, case_id, uid):
    return one(db, """SELECT d.uid, d.pod, r.service FROM drain_cases d
        JOIN restart_decisions r ON r.id=d.restart_id
        JOIN service_members m ON m.service=r.service AND m.pod=d.pod
        WHERE d.id=? AND d.status='drained' AND r.status='settled' AND m.uid=? AND m.state='recovering'""",
        (case_id, uid))


def evaluate_plan(manifest, chosen):
    """Return (consistent, preserved) for a {volume: set(txn)} selection."""
    preserved = set()
    for txn, info in manifest.items():
        present = [txn in chosen[volume] for volume in info['volumes']]
        if any(present) and not all(present):
            return False, preserved
        if all(present):
            preserved.add(txn)
    return True, preserved


def plan_propose(db, x):
    scope = recovery_scope(db, x['case'], x['uid'])
    if not scope:
        return {'decision': 'wrong_volume_recovery'}
    service, pod, source_uid = scope['service'], scope['pod'], scope['uid']
    manifest = manifest_txns(db, service, pod)
    if not manifest:
        return {'decision': 'manifest_required'}
    prior = one(db, 'SELECT * FROM recovery_plans WHERE id=?', (x['id'],))
    if prior:
        same = (prior['case_id'] == x['case'] and prior['uid'] == x['uid']
                and json.loads(prior['selections']) == x.get('selections'))
        if same:
            return {'decision': 'plan_approved', 'preserved': json.loads(prior['preserved'])}
        return {'decision': 'plan_conflict'}
    if one(db, "SELECT 1 FROM recovery_plans WHERE case_id=? AND status='approved'", (x['case'],)):
        return {'decision': 'plan_already_approved'}
    selections = x.get('selections')
    volumes_required = required(db, service, pod)
    if (not isinstance(selections, dict) or not volumes_required or set(selections) != volumes_required
            or any(not isinstance(snapshot, str) for snapshot in selections.values())):
        return {'decision': 'invalid_plan'}
    chosen = {}
    for volume in sorted(selections):
        snapshot = one(db, 'SELECT * FROM volume_snapshots WHERE id=?', (selections[volume],))
        if (not snapshot or snapshot['service'] != service or snapshot['pod'] != pod
                or snapshot['volume'] != volume or snapshot['source_uid'] != source_uid):
            return {'decision': 'wrong_volume_recovery'}
        if snapshot['withdrawn'] or snapshot['transactions'] is None:
            return {'decision': 'invalid_plan'}
        chosen[volume] = set(json.loads(snapshot['transactions']))
    consistent, preserved = evaluate_plan(manifest, chosen)
    if not consistent:
        return {'decision': 'plan_inconsistent'}
    committed = {txn for txn, info in manifest.items() if info['acked']}
    if not committed <= preserved:
        return {'decision': 'plan_drops_committed'}
    staging = set(manifest) - committed
    kept = preserved & staging
    candidates = {volume: rows(db, '''SELECT * FROM volume_snapshots
                                      WHERE service=? AND pod=? AND volume=? AND source_uid=?
                                      AND withdrawn=0 AND transactions IS NOT NULL ORDER BY id''',
                               (service, pod, volume, source_uid))
                  for volume in sorted(volumes_required)}
    for combo in itertools.product(*(candidates[volume] for volume in sorted(candidates))):
        other = {volume: set(json.loads(snapshot['transactions']))
                 for volume, snapshot in zip(sorted(candidates), combo)}
        feasible, other_preserved = evaluate_plan(manifest, other)
        if feasible and committed <= other_preserved and kept < (other_preserved & staging):
            return {'decision': 'plan_dominated'}
    db.execute('INSERT INTO recovery_plans VALUES(?,?,?,?,?,?,?,?,?)',
               (x['id'], x['case'], service, pod, source_uid, x['uid'],
                json.dumps(selections, sort_keys=True), json.dumps(sorted(preserved)), 'approved'))
    return {'decision': 'plan_approved', 'preserved': sorted(preserved)}


def verify(db, x):
    scope = recovery_scope(db, x['case'], x['uid'])
    snapshot = one(db, 'SELECT * FROM volume_snapshots WHERE id=?', (x['snapshot'],))
    if (not scope or not snapshot or snapshot['service'] != scope['service'] or snapshot['pod'] != scope['pod']
            or snapshot['source_uid'] != scope['uid'] or snapshot['volume'] != x['volume']):
        return {'decision': 'wrong_volume_recovery'}
    if manifest_txns(db, scope['service'], scope['pod']):
        plan = one(db, "SELECT * FROM recovery_plans WHERE case_id=? AND uid=? AND status='approved'",
                   (x['case'], x['uid']))
        if not plan:
            return {'decision': 'recovery_plan_required'}
        if json.loads(plan['selections']).get(x['volume']) != x['snapshot']:
            return {'decision': 'snapshot_not_in_plan'}
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
    if x['op'] == 'manifest-record':
        return record_manifest(db, x)
    if x['op'] == 'snapshot-record':
        return record_snapshot(db, x)
    if x['op'] == 'snapshot-withdraw':
        return withdraw_snapshot(db, x)
    if x['op'] == 'plan-propose':
        return plan_propose(db, x)
    if x['op'] == 'snapshot-verify':
        return verify(db, x)
    return {'decision': 'unknown_operation'}
