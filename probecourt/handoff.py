"""Single-placement handoff manifests for sessions leaving a drained container."""
from collections import Counter

from .store import one, rows


SCHEMA = '''
CREATE TABLE IF NOT EXISTS handoff_manifests(case_id TEXT PRIMARY KEY, manifest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS handoff_assignments(
    case_id TEXT NOT NULL, session TEXT NOT NULL, gateway TEXT NOT NULL,
    gateway_uid TEXT NOT NULL, units INTEGER NOT NULL, receipt TEXT,
    PRIMARY KEY(case_id, session)
);
'''


def _pending_case(db, x):
    return one(db, """SELECT c.* FROM drain_cases c
        JOIN restart_decisions r ON r.id=c.restart_id
        WHERE c.id=? AND c.uid=? AND c.status='pending' AND r.status='reserved'""",
        (x['case'], x['uid']))


def prepare(db, x):
    case = _pending_case(db, x)
    if not case or one(db, 'SELECT 1 FROM handoff_manifests WHERE case_id=?', (x['case'],)):
        return {'decision': 'handoff_unavailable'}
    pending = {row['session'] for row in rows(db, "SELECT session FROM session_receipts WHERE case_id=? AND status='pending'", (case['id'],))}
    gateways = x['gateways']
    assignments = x['assignments']
    if not pending or set(assignments) != pending:
        return {'decision': 'handoff_uncovered_sessions'}
    loads = Counter()
    for placement in assignments.values():
        gateway, units = placement['gateway'], placement['units']
        if gateway not in gateways or type(units) is not int or units <= 0:
            return {'decision': 'handoff_invalid_placement'}
        loads[gateway] += units
    if any(not info['uid'] or type(info['capacity']) is not int or info['capacity'] <= 0
           for info in gateways.values()):
        return {'decision': 'handoff_invalid_gateway'}
    if any(units > gateways[gateway]['capacity'] for gateway, units in loads.items()):
        return {'decision': 'handoff_over_capacity'}
    db.execute('INSERT INTO handoff_manifests VALUES(?,?)', (case['id'], x['event_id']))
    for session, placement in sorted(assignments.items()):
        gateway = placement['gateway']
        db.execute('INSERT INTO handoff_assignments VALUES(?,?,?,?,?,NULL)',
                   (case['id'], session, gateway, gateways[gateway]['uid'], placement['units']))
    return {'decision': 'handoff_placed', 'loads': dict(loads)}


def adopt(db, x, acknowledge):
    if not _pending_case(db, x):
        return {'decision': 'handoff_unavailable'}
    manifest = one(db, 'SELECT manifest FROM handoff_manifests WHERE case_id=?', (x['case'],))
    placement = one(db, 'SELECT * FROM handoff_assignments WHERE case_id=? AND session=?',
                    (x['case'], x['session']))
    if (not placement or placement['receipt'] is not None or not x['receipt'] or
            manifest['manifest'] != x['manifest'] or placement['gateway'] != x['gateway'] or
            placement['gateway_uid'] != x['gateway_uid']):
        return {'decision': 'handoff_receipt_mismatch'}
    db.execute('UPDATE handoff_assignments SET receipt=? WHERE case_id=? AND session=?',
               (x['receipt'], x['case'], x['session']))
    return acknowledge(db, x)
