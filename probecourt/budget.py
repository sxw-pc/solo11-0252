from .store import one
SCHEMA='''
CREATE TABLE IF NOT EXISTS restart_policies(service TEXT PRIMARY KEY,min_available INTEGER,capacity INTEGER,cooldown INTEGER);
CREATE TABLE IF NOT EXISTS restart_credits(service TEXT PRIMARY KEY,used INTEGER,revision INTEGER);
CREATE TABLE IF NOT EXISTS restart_decisions(id TEXT PRIMARY KEY,service TEXT,pod TEXT,uid TEXT,window_revision INTEGER,at INTEGER,status TEXT,receipt TEXT);
CREATE TABLE IF NOT EXISTS service_members(service TEXT,pod TEXT,uid TEXT,state TEXT,PRIMARY KEY(service,pod));
'''
def handle(db,x):
    op=x['op']
    if op=='policy':
        if x['capacity']<1 or x['min_available']<1:return {'decision':'invalid_policy'}
        db.execute('INSERT INTO restart_policies VALUES(?,?,?,?) ON CONFLICT(service) DO UPDATE SET min_available=excluded.min_available,capacity=excluded.capacity,cooldown=excluded.cooldown',(x['service'],x['min_available'],x['capacity'],x['cooldown']))
        db.execute('INSERT OR IGNORE INTO restart_credits VALUES(?,0,1)',(x['service'],))
        return {'decision':'policy_active'}
    if op=='member':
        old=one(db,'SELECT * FROM service_members WHERE service=? AND pod=?',(x['service'],x['pod']))
        if old and old['uid']==x['uid'] and old['state']==x['state']:return {'decision':'reused'}
        db.execute('INSERT INTO service_members VALUES(?,?,?,?) ON CONFLICT(service,pod) DO UPDATE SET uid=excluded.uid,state=excluded.state',(x['service'],x['pod'],x['uid'],x['state']))
        return {'decision':'member_recorded'}
    if op=='credit-reserve':
        p=one(db,'SELECT * FROM restart_policies WHERE service=?',(x['service'],))
        w=one(db,'SELECT * FROM health_windows WHERE pod=?',(x['pod'],))
        source=one(db,'SELECT * FROM probe_sources WHERE id=?',(w['source'],)) if w else None
        if not p or not w or not source or source['epoch']!=w['source_epoch'] or source['uid']!=w['uid'] or w['uid']!=x['uid'] or w['status']!='failed':return {'decision':'health_not_confirmed'}
        used=one(db,"SELECT COUNT(*) n FROM restart_decisions WHERE service=? AND status IN ('reserved','settled')",(x['service'],))['n']
        existing=one(db,"SELECT 1 FROM restart_decisions WHERE pod=? AND uid=? AND window_revision=? AND status IN ('reserved','settled')",(x['pod'],x['uid'],w['revision']))
        member=one(db,'SELECT * FROM service_members WHERE service=? AND pod=?',(x['service'],x['pod']))
        available=one(db,"SELECT COUNT(*) n FROM service_members WHERE service=? AND state='active'",(x['service'],))['n']
        if existing or not member or member['uid']!=x['uid'] or member['state']!='active' or used>=p['capacity'] or available-used-1<p['min_available']:
            return {'decision':'availability_budget_denied'}
        db.execute('INSERT INTO restart_decisions VALUES(?,?,?,?,?,?,?,?)',(x['id'],x['service'],x['pod'],x['uid'],w['revision'],x['at'],'reserved',None))
        db.execute('UPDATE restart_credits SET used=used+1,revision=revision+1 WHERE service=?',(x['service'],))
        return {'decision':'reserved','window_revision':w['revision']}
    if op=='restart-settle':
        d=one(db,'SELECT * FROM restart_decisions WHERE id=?',(x['id'],))
        w=one(db,'SELECT * FROM health_windows WHERE pod=?',(d['pod'],)) if d else None
        source=one(db,'SELECT * FROM probe_sources WHERE id=?',(w['source'],)) if w else None
        drained=one(db,"SELECT * FROM drain_cases WHERE restart_id=? AND uid=? AND status='drained'",(x['id'],x['old_uid'])) if d else None
        if not d or d['status']!='reserved' or not drained or not w or not source or source['epoch']!=w['source_epoch'] or source['uid']!=w['uid'] or w['uid']!=x['old_uid'] or w['revision']!=d['window_revision'] or not x.get('new_uid') or x['new_uid']==x['old_uid']:
            return {'decision':'stale_restart_receipt'}
        db.execute("UPDATE restart_decisions SET status='settled',receipt=? WHERE id=?",(x['receipt'],x['id']))
        db.execute("UPDATE health_windows SET uid=?,status='suspect',revision=revision+1 WHERE pod=?",(x['new_uid'],d['pod']))
        db.execute("UPDATE service_members SET uid=?,state='recovering' WHERE service=? AND pod=? AND uid=?",(x['new_uid'],d['service'],d['pod'],x['old_uid']))
        return {'decision':'restart_settled'}
    if op=='cooldown-release':
        d=one(db,'SELECT * FROM restart_decisions WHERE id=?',(x['id'],))
        p=one(db,'SELECT * FROM restart_policies WHERE service=?',(d['service'],)) if d else None
        w=one(db,'SELECT * FROM health_windows WHERE pod=?',(d['pod'],)) if d else None
        completed=one(db,"SELECT 1 FROM drain_cases WHERE restart_id=? AND status='complete'",(x['id'],)) if d else None
        member=one(db,'SELECT * FROM service_members WHERE service=? AND pod=?',(d['service'],d['pod'])) if d else None
        if not d or d['status']!='settled' or not p or not w or w['status']!='healthy' or not completed or not member or member['uid']!=w['uid'] or member['state']!='active' or x['at']<d['at']+p['cooldown']:
            return {'decision':'cooldown_blocked'}
        db.execute("UPDATE restart_decisions SET status='released' WHERE id=?",(x['id'],))
        db.execute('UPDATE restart_credits SET used=used-1,revision=revision+1 WHERE service=?',(d['service'],))
        return {'decision':'credit_restored'}
    return {'decision':'unknown_operation'}
