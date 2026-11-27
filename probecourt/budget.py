import json

from .store import one, rows

OFFSET_MIN = -10
OFFSET_MAX = 10
MAX_EXECUTION_WINDOWS = 2

SCHEMA = '''
CREATE TABLE IF NOT EXISTS restart_policies(service TEXT PRIMARY KEY,min_available INTEGER,capacity INTEGER,cooldown INTEGER);
CREATE TABLE IF NOT EXISTS restart_credits(service TEXT PRIMARY KEY,used INTEGER,revision INTEGER);
CREATE TABLE IF NOT EXISTS restart_decisions(id TEXT PRIMARY KEY,service TEXT,pod TEXT,uid TEXT,window_revision INTEGER,at INTEGER,status TEXT,receipt TEXT);
CREATE TABLE IF NOT EXISTS service_members(service TEXT,pod TEXT,uid TEXT,state TEXT,PRIMARY KEY(service,pod));
CREATE TABLE IF NOT EXISTS execution_windows(id TEXT PRIMARY KEY,restart_id TEXT,drain_id TEXT,uid TEXT,window_revision INTEGER,start INTEGER,finish INTEGER);
CREATE TABLE IF NOT EXISTS clock_calibrations(id TEXT PRIMARY KEY,restart_id TEXT,sender TEXT,offsets TEXT);
CREATE TABLE IF NOT EXISTS restart_receipt_reviews(restart_id TEXT PRIMARY KEY,receipt_event_id TEXT,receipt TEXT,sender TEXT,new_uid TEXT,start INTEGER,finish INTEGER,status TEXT,offsets TEXT,blocked TEXT);
'''


def _signed_offsets(db, restart_id):
    """Intersection of the signed valid-offset candidates of every calibration
    original already on file for this restart. Empty intersection means the
    signed originals contradict each other."""
    sets = []
    for c in rows(db, 'SELECT * FROM clock_calibrations WHERE restart_id=? ORDER BY id', (restart_id,)):
        sets.append(set(json.loads(c['offsets'])))
    if not sets:
        return set(range(OFFSET_MIN, OFFSET_MAX + 1))
    common = set(sets[0])
    for s in sets[1:]:
        common &= s
    return common


def _candidate_offsets(db, d, receipt):
    """Offsets compatible with the receipt's own candidates and every signed
    calibration original. Receipt candidates are bounded integers in [-10,10];
    a missing list defaults to the full bounded range."""
    raw = receipt.get('offset_candidates')
    if raw is None:
        own = set(range(OFFSET_MIN, OFFSET_MAX + 1))
    else:
        own = set(raw)
    own = {v for v in own if isinstance(v, int) and not isinstance(v, bool) and OFFSET_MIN <= v <= OFFSET_MAX}
    return own & _signed_offsets(db, d['id'])


def _windows_for(db, d, drain_id):
    return rows(
        db,
        'SELECT * FROM execution_windows WHERE restart_id=? AND drain_id=? AND uid=? ORDER BY start,id',
        (d['id'], drain_id, d['uid']),
    )


def _window_covers(window, start, finish, offset):
    """Half-open integer-clock interval [start+offset, finish+offset) must lie
    entirely inside one registered window [w_start, w_finish)."""
    return window['start'] <= start + offset and finish + offset <= window['finish']


def _post_settlement(db, d, new_uid, receipt):
    """The original business posting: consume the reservation, move the member
    to the new UID's recovering state. Timing evidence never alters it."""
    db.execute("UPDATE restart_decisions SET status='settled',receipt=? WHERE id=?", (receipt, d['id']))
    db.execute("UPDATE health_windows SET uid=?,status='suspect',revision=revision+1 WHERE pod=?", (new_uid, d['pod']))
    db.execute(
        "UPDATE service_members SET uid=?,state='recovering' WHERE service=? AND pod=? AND uid=?",
        (new_uid, d['service'], d['pod'], d['uid']),
    )


def _late_base(db, x):
    """Shared gates for a late original: an open decision on the old UID with a
    matching drained withdrawal order and a still-consistent failed window."""
    d = one(db, 'SELECT * FROM restart_decisions WHERE id=?', (x['id'],))
    if not d or d['status'] != 'reserved' or d['uid'] != x['old_uid']:
        return None, None, None
    drained = one(
        db,
        "SELECT * FROM drain_cases WHERE restart_id=? AND uid=? AND status='drained'",
        (x['id'], x['old_uid']),
    )
    w = one(db, 'SELECT * FROM health_windows WHERE pod=?', (d['pod'],))
    source = one(db, 'SELECT * FROM probe_sources WHERE id=?', (w['source'],)) if w else None
    if (
        not drained
        or not w
        or not source
        or source['epoch'] != w['source_epoch']
        or source['uid'] != w['uid']
        or w['uid'] != x['old_uid']
        or not x.get('new_uid')
        or x['new_uid'] == x['old_uid']
    ):
        return None, None, None
    return d, drained, w


def _timed_receipt(x):
    """A late runtime original carries a trusted sender, both UIDs and a
    start/finish pair stamped by one runtime clock."""
    if not x.get('sender'):
        return None
    start, finish = x.get('start'), x.get('finish')
    if (
        not isinstance(start, int)
        or isinstance(start, bool)
        or not isinstance(finish, int)
        or isinstance(finish, bool)
        or start > finish
    ):
        return None
    return start, finish, x['sender']


def handle(db, x):
    op = x['op']
    if op == 'policy':
        if x['capacity'] < 1 or x['min_available'] < 1:
            return {'decision': 'invalid_policy'}
        db.execute(
            'INSERT INTO restart_policies VALUES(?,?,?,?) ON CONFLICT(service) DO UPDATE SET min_available=excluded.min_available,capacity=excluded.capacity,cooldown=excluded.cooldown',
            (x['service'], x['min_available'], x['capacity'], x['cooldown']),
        )
        db.execute('INSERT OR IGNORE INTO restart_credits VALUES(?,0,1)', (x['service'],))
        return {'decision': 'policy_active'}
    if op == 'member':
        old = one(db, 'SELECT * FROM service_members WHERE service=? AND pod=?', (x['service'], x['pod']))
        if old and old['uid'] == x['uid'] and old['state'] == x['state']:
            return {'decision': 'reused'}
        db.execute(
            'INSERT INTO service_members VALUES(?,?,?,?) ON CONFLICT(service,pod) DO UPDATE SET uid=excluded.uid,state=excluded.state',
            (x['service'], x['pod'], x['uid'], x['state']),
        )
        return {'decision': 'member_recorded'}
    if op == 'credit-reserve':
        p = one(db, 'SELECT * FROM restart_policies WHERE service=?', (x['service'],))
        w = one(db, 'SELECT * FROM health_windows WHERE pod=?', (x['pod'],))
        source = one(db, 'SELECT * FROM probe_sources WHERE id=?', (w['source'],)) if w else None
        if not p or not w or not source or source['epoch'] != w['source_epoch'] or source['uid'] != w['uid'] or w['uid'] != x['uid'] or w['status'] != 'failed':
            return {'decision': 'health_not_confirmed'}
        used = one(db, "SELECT COUNT(*) n FROM restart_decisions WHERE service=? AND status IN ('reserved','settled')", (x['service'],))['n']
        existing = one(
            db,
            "SELECT 1 FROM restart_decisions WHERE pod=? AND uid=? AND window_revision=? AND status IN ('reserved','settled')",
            (x['pod'], x['uid'], w['revision']),
        )
        member = one(db, 'SELECT * FROM service_members WHERE service=? AND pod=?', (x['service'], x['pod']))
        available = one(db, "SELECT COUNT(*) n FROM service_members WHERE service=? AND state='active'", (x['service'],))['n']
        if existing or not member or member['uid'] != x['uid'] or member['state'] != 'active' or used >= p['capacity'] or available - used - 1 < p['min_available']:
            return {'decision': 'availability_budget_denied'}
        db.execute('INSERT INTO restart_decisions VALUES(?,?,?,?,?,?,?,?)', (x['id'], x['service'], x['pod'], x['uid'], w['revision'], x['at'], 'reserved', None))
        db.execute('UPDATE restart_credits SET used=used+1,revision=revision+1 WHERE service=?', (x['service'],))
        return {'decision': 'reserved', 'window_revision': w['revision']}
    if op == 'restart-settle':
        d = one(db, 'SELECT * FROM restart_decisions WHERE id=?', (x['id'],))
        w = one(db, 'SELECT * FROM health_windows WHERE pod=?', (d['pod'],)) if d else None
        source = one(db, 'SELECT * FROM probe_sources WHERE id=?', (w['source'],)) if w else None
        drained = one(db, "SELECT * FROM drain_cases WHERE restart_id=? AND uid=? AND status='drained'", (x['id'], x['old_uid'])) if d else None
        legacy = (
            d is not None
            and d['status'] == 'reserved'
            and drained is not None
            and w is not None
            and source is not None
            and source['epoch'] == w['source_epoch']
            and source['uid'] == w['uid']
            and w['uid'] == x['old_uid']
            and w['revision'] == d['window_revision']
            and x.get('new_uid')
            and x['new_uid'] != x['old_uid']
            and not _timed_receipt(x)
        )
        if legacy:
            _post_settlement(db, d, x['new_uid'], x['receipt'])
            return {'decision': 'restart_settled'}
        # Late original path: prove the restart actually happened inside a
        # sanctioned execution window despite the window revision change.
        d2, drained2, w2 = _late_base(db, x)
        if d2 is None:
            return {'decision': 'stale_restart_receipt'}
        timed = _timed_receipt(x)
        if timed is None:
            return {'decision': 'stale_restart_receipt'}
        start, finish, sender = timed
        candidates = _candidate_offsets(db, d2, x)
        if not candidates:
            return {'decision': 'calibration_conflict'}
        windows = _windows_for(db, d2, drained2['id'])
        blocked = []
        for offset in sorted(candidates):
            cover = next((win for win in windows if _window_covers(win, start, finish, offset)), None)
            if cover is None:
                blocked.append(
                    {
                        'offset': offset,
                        'interval': [start + offset, finish + offset],
                        'windows': [[win['start'], win['finish']] for win in windows],
                    }
                )
        if not blocked:
            _post_settlement(db, d2, x['new_uid'], x['receipt'])
            return {
                'decision': 'restart_settled',
                'offsets': sorted(candidates),
                'windows': [[win['start'], win['finish']] for win in windows],
            }
        # No common proof yet: keep the reservation and old membership, park one
        # review todo carrying the concrete offsets/intervals that block it.
        existing_review = one(db, 'SELECT * FROM restart_receipt_reviews WHERE restart_id=?', (d2['id'],))
        if existing_review is not None:
            return {
                'decision': 'review_pending',
                'blocked': json.loads(existing_review['blocked']),
            }
        db.execute(
            'INSERT INTO restart_receipt_reviews VALUES(?,?,?,?,?,?,?,?,?,?)',
            (
                d2['id'],
                x['event_id'],
                x['receipt'],
                sender,
                x['new_uid'],
                start,
                finish,
                'pending',
                _json(sorted(candidates)),
                _json(blocked),
            ),
        )
        return {'decision': 'review_pending', 'blocked': blocked}
    if op == 'execution-window':
        # At most two windows per reserved decision; registerable only after the
        # real drain order is drained and only while the old UID is still open.
        if one(db, 'SELECT 1 FROM execution_windows WHERE id=?', (x['id'],)):
            return {'decision': 'stale_restart_receipt'}
        d = one(db, 'SELECT * FROM restart_decisions WHERE id=?', (x['restart'],))
        case = one(db, 'SELECT * FROM drain_cases WHERE id=?', (x['drain'],)) if d else None
        w = one(db, 'SELECT * FROM health_windows WHERE pod=?', (d['pod'],)) if d else None
        start, finish = x.get('start'), x.get('finish')
        if (
            not d
            or d['status'] != 'reserved'
            or d['uid'] != x['uid']
            or not case
            or case['restart_id'] != d['id']
            or case['uid'] != d['uid']
            or case['status'] != 'drained'
            or not w
            or w['uid'] != d['uid']
            or w['status'] != 'failed'
            or not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(finish, int)
            or isinstance(finish, bool)
            or start >= finish
        ):
            return {'decision': 'stale_restart_receipt'}
        count = one(db, 'SELECT COUNT(*) n FROM execution_windows WHERE restart_id=?', (d['id'],))['n']
        if count >= MAX_EXECUTION_WINDOWS:
            return {'decision': 'execution_window_limit'}
        db.execute(
            'INSERT INTO execution_windows VALUES(?,?,?,?,?,?,?)',
            (x['id'], d['id'], case['id'], d['uid'], w['revision'], start, finish),
        )
        return {'decision': 'execution_window_registered', 'window_revision': w['revision'], 'window': [start, finish]}
    if op == 'clock-calibration':
        # A signed time-correction original; its valid-offset candidates may
        # contain holes. Multiple originals intersected, an empty intersection
        # is a contradiction and never posts.
        offsets = x.get('offset_candidates')
        if (
            not offsets
            or not isinstance(offsets, list)
            or any(not isinstance(v, int) or isinstance(v, bool) or v < OFFSET_MIN or v > OFFSET_MAX for v in offsets)
            or len(set(offsets)) != len(offsets)
        ):
            return {'decision': 'calibration_conflict'}
        d = one(db, 'SELECT * FROM restart_decisions WHERE id=?', (x['restart'],))
        if not d or d['uid'] != x['uid'] or not x.get('sender'):
            return {'decision': 'stale_restart_receipt'}
        review = one(db, 'SELECT * FROM restart_receipt_reviews WHERE restart_id=?', (d['id'],))
        if review is not None:
            # Only the single bound review-fetch intake may change a parked case.
            return {'decision': 'review_pending' if review['status'] == 'pending' else 'review_closed'}
        if d['status'] != 'reserved':
            return {'decision': 'stale_restart_receipt'}
        if not (set(offsets) & _signed_offsets(db, d['id'])):
            return {'decision': 'calibration_conflict'}
        db.execute(
            'INSERT INTO clock_calibrations VALUES(?,?,?,?)',
            (x['id'], d['id'], x['sender'], _json(sorted(offsets))),
        )
        return {'decision': 'calibration_recorded', 'offsets': sorted(offsets)}
    if op == 'review-fetch':
        # The one and only follow-up intake bound to a parked late original.
        review = one(db, 'SELECT * FROM restart_receipt_reviews WHERE restart_id=?', (x['restart'],))
        if review is None or not x.get('sender'):
            return {'decision': 'stale_restart_receipt'}
        if review['status'] != 'pending':
            return {'decision': 'review_closed'}
        offsets = x.get('offset_candidates')
        if (
            not offsets
            or not isinstance(offsets, list)
            or any(not isinstance(v, int) or isinstance(v, bool) or v < OFFSET_MIN or v > OFFSET_MAX for v in offsets)
            or len(set(offsets)) != len(offsets)
            or not (set(offsets) & _signed_offsets(db, review['restart_id']))
        ):
            return {'decision': 'calibration_conflict'}
        d = one(db, 'SELECT * FROM restart_decisions WHERE id=?', (review['restart_id'],))
        if not d or d['status'] != 'reserved':
            return {'decision': 'stale_restart_receipt'}
        drained = one(
            db,
            "SELECT * FROM drain_cases WHERE restart_id=? AND uid=? AND status='drained'",
            (d['id'], d['uid']),
        )
        if drained is None:
            return {'decision': 'stale_restart_receipt'}
        db.execute(
            'INSERT INTO clock_calibrations VALUES(?,?,?,?)',
            (x['event_id'], d['id'], x['sender'], _json(sorted(offsets))),
        )
        candidates = set(json.loads(review['offsets'])) & set(offsets) & _signed_offsets(db, d['id'])
        windows = _windows_for(db, d, drained['id'])
        blocked = []
        for offset in sorted(candidates):
            cover = next((win for win in windows if _window_covers(win, review['start'], review['finish'], offset)), None)
            if cover is None:
                blocked.append(
                    {
                        'offset': offset,
                        'interval': [review['start'] + offset, review['finish'] + offset],
                        'windows': [[win['start'], win['finish']] for win in windows],
                    }
                )
        if not blocked and candidates:
            _post_settlement(db, d, review['new_uid'], review['receipt'])
            db.execute(
                "UPDATE restart_receipt_reviews SET status='approved',blocked=? WHERE restart_id=?",
                (_json(blocked), d['id']),
            )
            return {
                'decision': 'review_approved',
                'offsets': sorted(candidates),
                'windows': [[win['start'], win['finish']] for win in windows],
            }
        db.execute(
            "UPDATE restart_receipt_reviews SET status='denied',offsets=?,blocked=? WHERE restart_id=?",
            (_json(sorted(candidates)), _json(blocked), d['id']),
        )
        return {'decision': 'review_denied', 'blocked': blocked}
    if op == 'cooldown-release':
        d = one(db, 'SELECT * FROM restart_decisions WHERE id=?', (x['id'],))
        p = one(db, 'SELECT * FROM restart_policies WHERE service=?', (d['service'],)) if d else None
        w = one(db, 'SELECT * FROM health_windows WHERE pod=?', (d['pod'],)) if d else None
        completed = one(db, "SELECT 1 FROM drain_cases WHERE restart_id=? AND status='complete'", (x['id'],)) if d else None
        member = one(db, 'SELECT * FROM service_members WHERE service=? AND pod=?', (d['service'], d['pod'])) if d else None
        if not d or d['status'] != 'settled' or not p or not w or w['status'] != 'healthy' or not completed or not member or member['uid'] != w['uid'] or member['state'] != 'active' or x['at'] < d['at'] + p['cooldown']:
            return {'decision': 'cooldown_blocked'}
        db.execute("UPDATE restart_decisions SET status='released' WHERE id=?", (x['id'],))
        db.execute('UPDATE restart_credits SET used=used-1,revision=revision+1 WHERE service=?', (d['service'],))
        return {'decision': 'credit_restored'}
    return {'decision': 'unknown_operation'}


def _json(values):
    return json.dumps(values, sort_keys=True, ensure_ascii=False)
