from .store import one
from . import handoff
SCHEMA='''
CREATE TABLE IF NOT EXISTS drain_cases(id TEXT PRIMARY KEY,pod TEXT,uid TEXT,restart_id TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS session_receipts(id TEXT PRIMARY KEY,case_id TEXT,session TEXT,uid TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS traffic_decisions(id TEXT PRIMARY KEY,pod TEXT,uid TEXT,status TEXT,at INTEGER);
''' + handoff.SCHEMA

def acknowledge(db, x):
    case=one(db,'SELECT * FROM drain_cases WHERE id=?',(x['case'],))
    if not case or case['uid']!=x['uid']:return {'decision':'stale_session_ack'}
    old=one(db,'SELECT * FROM session_receipts WHERE case_id=? AND session=?',(x['case'],x['session']))
    if not old or old['status']!='pending':return {'decision':'session_not_pending'}
    db.execute("UPDATE session_receipts SET status='cleared' WHERE id=?",(old['id'],))
    count=one(db,"SELECT COUNT(*) n FROM session_receipts WHERE case_id=? AND status='pending'",(x['case'],))['n']
    if count==0:db.execute("UPDATE drain_cases SET status='drained' WHERE id=?",(x['case'],))
    return {'decision':'drained' if count==0 else 'partially_drained','pending':count}

def handle(db,x):
    op=x['op']
    if op=='session-drain':
        d=one(db,'SELECT * FROM restart_decisions WHERE id=?',(x['restart'],))
        if not d or d['status']!='reserved' or d['uid']!=x['uid']:return {'decision':'restart_not_reserved'}
        db.execute('INSERT INTO drain_cases VALUES(?,?,?,?,?)',(x['id'],d['pod'],x['uid'],d['id'],'pending'))
        for sid in x['sessions']:
            db.execute('INSERT INTO session_receipts VALUES(?,?,?,?,?)',(x['id']+':'+sid,x['id'],sid,x['uid'],'pending'))
        return {'decision':'traffic_withdrawn','pending':len(x['sessions'])}
    if op=='handoff-manifest':
        return handoff.prepare(db, x)
    if op=='session-adopt':
        return handoff.adopt(db, x, acknowledge)
    if op=='session-ack':
        if one(db, 'SELECT 1 FROM handoff_manifests WHERE case_id=?', (x['case'],)):
            return {'decision':'handoff_receipt_required'}
        return acknowledge(db, x)
    if op=='traffic-restore':
        case=one(db,'SELECT * FROM drain_cases WHERE id=?',(x['case'],))
        d=one(db,'SELECT * FROM restart_decisions WHERE id=?',(case['restart_id'],)) if case else None
        w=one(db,'SELECT * FROM health_windows WHERE pod=?',(case['pod'],)) if case else None
        if not case or case['status']!='drained' or not d or d['status']!='settled' or not w or w['uid']!=x['new_uid'] or w['status']!='healthy':
            return {'decision':'traffic_still_withdrawn'}
        db.execute('INSERT INTO traffic_decisions VALUES(?,?,?,?,?)',(x['id'],case['pod'],x['new_uid'],'restored',x['at']))
        db.execute("UPDATE drain_cases SET status='complete' WHERE id=?",(x['case'],))
        db.execute("UPDATE service_members SET state='active' WHERE service=? AND pod=? AND uid=?",(d['service'],case['pod'],x['new_uid']))
        return {'decision':'traffic_restored'}
    return {'decision':'unknown_operation'}
