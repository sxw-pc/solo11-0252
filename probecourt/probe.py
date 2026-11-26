from .store import one,rows
SCHEMA='''
CREATE TABLE IF NOT EXISTS probe_sources(id TEXT PRIMARY KEY,pod TEXT,uid TEXT,epoch INTEGER,state TEXT);
CREATE TABLE IF NOT EXISTS probe_samples(id TEXT PRIMARY KEY,source TEXT,epoch INTEGER,seq INTEGER,healthy INTEGER,at INTEGER);
CREATE TABLE IF NOT EXISTS health_windows(pod TEXT PRIMARY KEY,uid TEXT,source TEXT,source_epoch INTEGER,revision INTEGER,status TEXT,reason TEXT);
'''
def handle(db,x):
    op=x['op']
    if op=='source-register':
        old=one(db,'SELECT * FROM probe_sources WHERE id=?',(x['id'],))
        if old and x['epoch']<=old['epoch']:return {'decision':'stale_source'}
        db.execute('INSERT INTO probe_sources VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET pod=excluded.pod,uid=excluded.uid,epoch=excluded.epoch,state=excluded.state',(x['id'],x['pod'],x['uid'],x['epoch'],'active'))
        return {'decision':'source_active'}
    if op=='sample-record':
        s=one(db,'SELECT * FROM probe_sources WHERE id=?',(x['source'],))
        if not s or s['epoch']!=x['epoch'] or s['state']!='active':return {'decision':'stale_probe_source'}
        last=one(db,'SELECT MAX(seq) n FROM probe_samples WHERE source=? AND epoch=?',(x['source'],x['epoch']))['n']
        if last is not None and x['seq']<=last:return {'decision':'stale_sample'}
        db.execute('INSERT INTO probe_samples VALUES(?,?,?,?,?,?)',(x['id'],x['source'],x['epoch'],x['seq'],int(x['healthy']),x['at']))
        return {'decision':'sampled'}
    if op=='window-evaluate':
        source=one(db,'SELECT * FROM probe_sources WHERE id=?',(x['source'],))
        if not source:return {'decision':'unknown_source'}
        samples=rows(db,'SELECT * FROM probe_samples WHERE source=? AND epoch=? ORDER BY seq DESC LIMIT 2',(x['source'],source['epoch']))
        if len(samples)<2:return {'decision':'insufficient_samples'}
        other=one(db,"SELECT 1 FROM probe_samples s JOIN probe_sources p ON p.id=s.source WHERE p.pod=? AND p.uid=? AND p.id<>? AND p.state='active' AND s.healthy<>? AND s.at>=? LIMIT 1",(source['pod'],source['uid'],source['id'],samples[0]['healthy'],samples[1]['at']))
        status='conflict' if other else ('healthy' if all(s['healthy'] for s in samples) else 'failed' if not any(s['healthy'] for s in samples) else 'suspect')
        old=one(db,'SELECT * FROM health_windows WHERE pod=?',(source['pod'],))
        revision=(old['revision']+1) if old else 1
        db.execute('INSERT INTO health_windows VALUES(?,?,?,?,?,?,?) ON CONFLICT(pod) DO UPDATE SET uid=excluded.uid,source=excluded.source,source_epoch=excluded.source_epoch,revision=excluded.revision,status=excluded.status,reason=excluded.reason',(source['pod'],source['uid'],source['id'],source['epoch'],revision,status,x['source']))
        return {'decision':status,'revision':revision}
    if op=='evidence-review':
        w=one(db,'SELECT * FROM health_windows WHERE pod=?',(x['pod'],))
        if not w or w['status']!='conflict' or x.get('uid')!=w['uid']:return {'decision':'review_unavailable'}
        db.execute("UPDATE health_windows SET status='suspect',revision=revision+1,reason='manual_review' WHERE pod=?",(x['pod'],))
        db.execute("UPDATE restart_decisions SET status='cancelled' WHERE pod=? AND status='reserved'",(x['pod'],))
        return {'decision':'restart_cancelled_for_conflict'}
    return {'decision':'unknown_operation'}
