"""Single-placement handoff manifests and fault-committed primary/backup handoffs.

A normal manifest commits one receiver per session and only checks the
all-online layout.  A fault-committed manifest fixes, before the first adopt
receipt, both a primary and a distinct backup gateway per session and proves
capacity for the all-online scenario and every single-gateway outage.
"""
import json
from collections import Counter

from .store import one, rows


SCHEMA = '''
CREATE TABLE IF NOT EXISTS handoff_manifests(case_id TEXT PRIMARY KEY, manifest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS handoff_assignments(
    case_id TEXT NOT NULL, session TEXT NOT NULL, gateway TEXT NOT NULL,
    gateway_uid TEXT NOT NULL, units INTEGER NOT NULL, receipt TEXT,
    PRIMARY KEY(case_id, session)
);
CREATE TABLE IF NOT EXISTS handoff_guarantee_manifests(
    case_id TEXT PRIMARY KEY, manifest TEXT NOT NULL, plan TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS handoff_failover_assignments(
    case_id TEXT NOT NULL, session TEXT NOT NULL,
    primary_gateway TEXT NOT NULL, primary_uid TEXT NOT NULL,
    backup_gateway TEXT NOT NULL, backup_uid TEXT NOT NULL,
    units INTEGER NOT NULL, receipt TEXT,
    PRIMARY KEY(case_id, session)
);
CREATE TABLE IF NOT EXISTS handoff_offline_gateways(
    case_id TEXT NOT NULL, gateway TEXT NOT NULL,
    PRIMARY KEY(case_id, gateway)
);
'''

MAX_SESSIONS = 8
MAX_GATEWAYS = 4


def _pending_case(db, x):
    return one(db, """SELECT c.* FROM drain_cases c
        JOIN restart_decisions r ON r.id=c.restart_id
        WHERE c.id=? AND c.uid=? AND c.status='pending' AND r.status='reserved'""",
        (x['case'], x['uid']))


def _pending_sessions(db, case_id):
    return {row['session'] for row in rows(
        db, "SELECT session FROM session_receipts WHERE case_id=? AND status='pending'", (case_id,))}


def _offline(db, case_id):
    return {row['gateway'] for row in rows(
        db, 'SELECT gateway FROM handoff_offline_gateways WHERE case_id=?', (case_id,))}


def _valid_gateways(gateways):
    return isinstance(gateways, dict) and 0 < len(gateways) <= MAX_GATEWAYS and all(
        isinstance(info, dict) and info.get('uid') and
        type(info.get('capacity')) is int and info['capacity'] > 0
        for info in gateways.values())


def _scenario_target(pair, offline_gateway):
    """Gateway that owns a session in a given scenario (None means all online)."""
    if offline_gateway is not None and pair['primary'] == offline_gateway:
        return pair['backup']
    return pair['primary']


def _scenario_load_report(sessions, plan, gateways):
    report = []
    for offline_gateway in [None] + sorted(gateways):
        loads = Counter()
        for sid, units in sessions.items():
            loads[_scenario_target(plan[sid], offline_gateway)] += units
        report.append({'offline': offline_gateway, 'loads': dict(sorted(loads.items()))})
    return report


def _plan_violation(sessions, plan, gateways):
    """First capacity violation of an explicit plan across all scenarios."""
    for entry in _scenario_load_report(sessions, plan, gateways):
        over = {gw: units for gw, units in entry['loads'].items()
                if units > gateways[gw]['capacity']}
        if over:
            return {'offline': entry['offline'], 'loads': entry['loads'], 'over_capacity': over}
    return None


def _pack_scenario(sessions, eligible, gateways, offline_gateway):
    """Exact-capacity packer for one scenario: place every session on a
    surviving eligible gateway without exceeding signed capacity."""
    order = sorted(sessions, key=lambda sid: (-sessions[sid], sid))
    loads = Counter()
    placement = {}

    def backtrack(index):
        if index == len(order):
            return True
        sid = order[index]
        units = sessions[sid]
        for gw in sorted(g for g in eligible[sid] if g != offline_gateway):
            if loads[gw] + units <= gateways[gw]['capacity']:
                loads[gw] += units
                placement[sid] = gw
                if backtrack(index + 1):
                    return True
                loads[gw] -= units
                del placement[sid]
        return False

    feasible = backtrack(0)
    return {
        'offline': offline_gateway,
        'feasible': feasible,
        'placement': dict(sorted(placement.items())),
        'loads': dict(sorted(loads.items())),
        'unplaced': [sid for sid in order if sid not in placement],
    }


def _scenario_evidence(sessions, eligible, gateways):
    return [_pack_scenario(sessions, eligible, gateways, offline_gateway)
            for offline_gateway in [None] + sorted(gateways)]


def _solve_plan(sessions, eligible, gateways):
    """Joint primary/backup commitment search.

    A candidate pair is accepted only if it keeps every gateway within
    capacity in the all-online scenario and in every single-gateway outage,
    where a primary outage moves the session to its fixed backup while
    surviving primaries keep their sessions.
    """
    order = sorted(sessions, key=lambda sid: (-sessions[sid], sid))
    scenarios = [None] + sorted(gateways)
    loads = {scenario: Counter() for scenario in scenarios}
    plan = {}

    def backtrack(index):
        if index == len(order):
            return dict(plan)
        sid = order[index]
        units = sessions[sid]
        for primary in sorted(eligible[sid]):
            if loads[None][primary] + units > gateways[primary]['capacity']:
                continue
            for backup in sorted(gw for gw in eligible[sid] if gw != primary):
                ok = True
                for offline_gateway in gateways:
                    target = _scenario_target({'primary': primary, 'backup': backup},
                                              offline_gateway)
                    if loads[offline_gateway][target] + units > gateways[target]['capacity']:
                        ok = False
                        break
                if not ok:
                    continue
                loads[None][primary] += units
                for offline_gateway in gateways:
                    target = _scenario_target({'primary': primary, 'backup': backup},
                                              offline_gateway)
                    loads[offline_gateway][target] += units
                plan[sid] = {'primary': primary, 'backup': backup}
                found = backtrack(index + 1)
                if found:
                    return found
                del plan[sid]
                loads[None][primary] -= units
                for offline_gateway in gateways:
                    target = _scenario_target({'primary': primary, 'backup': backup},
                                              offline_gateway)
                    loads[offline_gateway][target] -= units
        return None

    return backtrack(0)


def _validate_guarantee_input(x, pending):
    gateways = x.get('gateways')
    sessions = x.get('sessions')
    eligible = x.get('eligible')
    if not _valid_gateways(gateways):
        return {'decision': 'handoff_invalid_gateway'}
    if not isinstance(sessions, dict) or set(sessions) != pending or not sessions:
        return {'decision': 'handoff_uncovered_sessions'}
    if len(pending) > MAX_SESSIONS:
        return {'decision': 'handoff_invalid_placement'}
    if any(type(units) is not int or units <= 0 for units in sessions.values()):
        return {'decision': 'handoff_invalid_placement'}
    if not isinstance(eligible, dict) or set(eligible) != pending:
        return {'decision': 'handoff_invalid_eligible'}
    for sid in pending:
        ends = eligible[sid]
        if (not isinstance(ends, list) or len(ends) < 2 or len(set(ends)) != len(ends)
                or any(gw not in gateways for gw in ends)):
            return {'decision': 'handoff_invalid_eligible'}
    return None


def _normalize_proposed_plan(x, pending, eligible):
    proposed = x.get('plan')
    if not isinstance(proposed, dict) or set(proposed) != pending:
        return None, {'decision': 'handoff_plan_invalid'}
    plan = {}
    for sid in pending:
        pair = proposed[sid]
        if (not isinstance(pair, dict) or
                pair.get('primary') not in eligible[sid] or
                pair.get('backup') not in eligible[sid] or
                pair['primary'] == pair['backup']):
            return None, {'decision': 'handoff_plan_invalid'}
        plan[sid] = {'primary': pair['primary'], 'backup': pair['backup']}
    return plan, None


def _store_guarantee(db, case_id, manifest_id, sessions, plan, gateways):
    db.execute('INSERT INTO handoff_guarantee_manifests VALUES(?,?,?)',
               (case_id, manifest_id, json.dumps(plan, sort_keys=True, ensure_ascii=False)))
    for sid in sorted(plan):
        pair = plan[sid]
        db.execute('INSERT INTO handoff_failover_assignments VALUES(?,?,?,?,?,?,?,NULL)',
                   (case_id, sid, pair['primary'], gateways[pair['primary']]['uid'],
                    pair['backup'], gateways[pair['backup']]['uid'], sessions[sid]))


def prepare_guarantee(db, x, case):
    if one(db, 'SELECT 1 FROM handoff_guarantee_manifests WHERE case_id=?', (x['case'],)):
        return {'decision': 'handoff_unavailable'}
    pending = _pending_sessions(db, case['id'])
    error = _validate_guarantee_input(x, pending)
    if error:
        return error
    gateways, sessions, eligible = x['gateways'], x['sessions'], x['eligible']

    if 'plan' in x:
        plan, error = _normalize_proposed_plan(x, pending, eligible)
        if error:
            return error
        violation = _plan_violation(sessions, plan, gateways)
        if violation:
            return {'decision': 'handoff_plan_violates_guarantee', 'violation': violation,
                    'evidence': _scenario_evidence(sessions, eligible, gateways)}
    else:
        plan = _solve_plan(sessions, eligible, gateways)
        if plan is None:
            evidence = _scenario_evidence(sessions, eligible, gateways)
            if any(not entry['feasible'] for entry in evidence):
                return {'decision': 'handoff_scenario_infeasible', 'evidence': evidence}
            return {'decision': 'handoff_guarantee_unsatisfiable', 'evidence': evidence}

    _store_guarantee(db, case['id'], x['event_id'], sessions, plan, gateways)
    report = _scenario_load_report(sessions, plan, gateways)
    return {'decision': 'handoff_guarantee_placed', 'plan': plan,
            'loads': report[0]['loads'], 'scenario_loads': report}


def report_offline(db, x):
    case = _pending_case(db, x)
    if not case or not one(db, 'SELECT 1 FROM handoff_guarantee_manifests WHERE case_id=?',
                           (x['case'],)):
        return {'decision': 'handoff_unavailable'}
    known = set()
    for row in rows(db, 'SELECT primary_gateway, backup_gateway FROM handoff_failover_assignments '
                         'WHERE case_id=?', (x['case'],)):
        known.add(row['primary_gateway'])
        known.add(row['backup_gateway'])
    gateway = x.get('gateway')
    if gateway not in known:
        return {'decision': 'handoff_gateway_unknown'}
    if one(db, 'SELECT 1 FROM handoff_offline_gateways WHERE case_id=? AND gateway=?',
           (x['case'], gateway)):
        return {'decision': 'gateway_already_offline', 'gateway': gateway}

    activated = [row['session'] for row in rows(
        db, 'SELECT session FROM handoff_failover_assignments '
            'WHERE case_id=? AND primary_gateway=? ORDER BY session', (x['case'], gateway))]
    already_offline = _offline(db, x['case'])
    db.execute('INSERT INTO handoff_offline_gateways VALUES(?,?)', (x['case'], gateway))
    # Sessions whose fixed backup is itself already lost have no reachable
    # committed receiver and cannot be adopted until the guarantee is redrawn.
    orphaned = [row['session'] for row in rows(
        db, 'SELECT session FROM handoff_failover_assignments '
            'WHERE case_id=? AND primary_gateway=? AND backup_gateway IN (%s) ORDER BY session'
        % ','.join('?' * len(already_offline)),
        (x['case'], gateway, *sorted(already_offline)))] if already_offline else []
    # Adopts recorded at the lost primary can no longer clear those sessions.
    db.execute("""UPDATE session_receipts SET status='pending'
        WHERE case_id=? AND status='cleared' AND session IN (
            SELECT session FROM handoff_failover_assignments
            WHERE case_id=? AND primary_gateway=?)""", (x['case'], x['case'], gateway))
    db.execute('UPDATE handoff_failover_assignments SET receipt=NULL '
               'WHERE case_id=? AND primary_gateway=?', (x['case'], gateway))
    pending = one(db, "SELECT COUNT(*) n FROM session_receipts WHERE case_id=? AND status='pending'",
                  (x['case'],))['n']
    if pending:
        db.execute("UPDATE drain_cases SET status='pending' WHERE id=?", (x['case'],))
    result = {'decision': 'gateway_offline_recorded', 'gateway': gateway,
              'activated_failovers': activated, 'pending': pending}
    if orphaned:
        result['orphaned_sessions'] = orphaned
    return result


def _current_target(assignment, offline):
    if assignment['primary_gateway'] in offline:
        if assignment['backup_gateway'] in offline:
            return None, None
        return assignment['backup_gateway'], assignment['backup_uid']
    return assignment['primary_gateway'], assignment['primary_uid']


def _adopt_guarantee(db, x, acknowledge, manifest_id):
    assignment = one(db, 'SELECT * FROM handoff_failover_assignments WHERE case_id=? AND session=?',
                     (x['case'], x['session']))
    target_gateway = target_uid = None
    if assignment:
        target_gateway, target_uid = _current_target(assignment, _offline(db, x['case']))
    if (not assignment or assignment['receipt'] is not None or not x.get('receipt') or
            manifest_id != x.get('manifest') or
            x.get('gateway') != target_gateway or x.get('gateway_uid') != target_uid):
        return {'decision': 'handoff_receipt_mismatch'}
    db.execute('UPDATE handoff_failover_assignments SET receipt=? WHERE case_id=? AND session=?',
               (x['receipt'], x['case'], x['session']))
    return acknowledge(db, x)


def _adopt_legacy(db, x, acknowledge):
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


def prepare(db, x):
    case = _pending_case(db, x)
    if not case or one(db, 'SELECT 1 FROM handoff_manifests WHERE case_id=?', (x['case'],)):
        return {'decision': 'handoff_unavailable'}
    if 'eligible' in x:
        return prepare_guarantee(db, x, case)
    pending = _pending_sessions(db, case['id'])
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
    guarantee = one(db, 'SELECT manifest FROM handoff_guarantee_manifests WHERE case_id=?',
                    (x['case'],))
    if guarantee:
        return _adopt_guarantee(db, x, acknowledge, guarantee['manifest'])
    if not one(db, 'SELECT 1 FROM handoff_manifests WHERE case_id=?', (x['case'],)):
        return {'decision': 'handoff_receipt_mismatch'}
    return _adopt_legacy(db, x, acknowledge)
