import json,tempfile,unittest
from pathlib import Path
import probecourt
from probecourt.store import connect,run,replay,one
ROOT=Path(__file__).resolve().parents[1]
class Business(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'state.sqlite';self.db=connect(str(self.path))
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def step(self,c,op,tag,**kw):return run(self.db,c,{'op':op,'event_id':tag,**kw})
 def path_run(self):
  return [run(self.db,x['command'],x['input']) for x in json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))]
 def test_cross_system_restart_and_traffic(self):
  r=self.path_run();self.assertEqual(r[3]['decision'],'failed');self.assertEqual(r[7]['decision'],'reserved')
  self.assertEqual(r[10]['decision'],'traffic_still_withdrawn');self.assertEqual(r[17]['decision'],'traffic_restored')
  self.assertEqual(r[-1]['decision'],'credit_restored');self.assertTrue(replay(str(self.path))['verified'])
 def test_stale_source_and_sample_cannot_rewrite_window(self):
  self.path_run();self.assertEqual(self.step('probe','sample-record','late',id='s0',source='src',epoch=1,seq=3,healthy=True,at=30)['decision'],'stale_probe_source')
  self.assertEqual(one(self.db,'SELECT status FROM health_windows WHERE pod=?',('pod-a',))['status'],'healthy')
 def test_budget_denies_last_available_replica(self):
  prefix=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))[:7]
  for x in prefix:run(self.db,x['command'],x['input'])
  self.step('budget','member','inactive',service='svc-a',pod='pod-b',uid='uid-b',state='inactive')
  self.assertEqual(self.step('budget','credit-reserve','deny',id='r2',service='svc-a',pod='pod-a',uid='uid-1',available=100,at=12)['decision'],'availability_budget_denied')
 def test_old_session_ack_cannot_complete_new_case(self):
  r=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))[:9]
  for x in r:run(self.db,x['command'],x['input'])
  self.assertEqual(self.step('traffic','session-ack','wrong',case='d1',uid='uid-2',session='session-a')['decision'],'stale_session_ack')
 def test_probe_conflict_cancels_reserved_restart(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:8]:run(self.db,x['command'],x['input'])
  self.step('probe','source-register','other-source',id='other',pod='pod-a',uid='uid-1',epoch=1)
  self.step('probe','sample-record','other-good',id='good',source='other',epoch=1,seq=1,healthy=True,at=11)
  self.assertEqual(self.step('probe','window-evaluate','conflict',source='src')['decision'],'conflict')
  self.assertEqual(self.step('probe','evidence-review','review',pod='pod-a',uid='uid-1')['decision'],'restart_cancelled_for_conflict')
  self.assertEqual(one(self.db,'SELECT status FROM restart_decisions WHERE id=?',('r1',))['status'],'cancelled')
 def test_stale_restart_and_premature_cooldown(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:9]:run(self.db,x['command'],x['input'])
  self.assertEqual(self.step('budget','restart-settle','bad-restart',id='r1',old_uid='wrong',new_uid='uid-2',receipt='bad')['decision'],'stale_restart_receipt')
  for x in p[9:18]:run(self.db,x['command'],x['input'])
  self.assertEqual(self.step('budget','cooldown-release','early',id='r1',at=13)['decision'],'cooldown_blocked')
 def test_restart_cannot_settle_before_all_sessions_drain(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:10]:run(self.db,x['command'],x['input'])
  settle=dict(p[12]['input'],event_id='premature-restart')
  self.assertEqual(run(self.db,'budget',settle)['decision'],'stale_restart_receipt')
  self.assertEqual(one(self.db,'SELECT uid FROM service_members WHERE service=? AND pod=?',('svc-a','pod-a'))['uid'],'uid-1')
 def test_restart_member_is_recovering_until_traffic_restore(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:13]:run(self.db,x['command'],x['input'])
  self.assertEqual(one(self.db,'SELECT uid FROM service_members WHERE service=? AND pod=?',('svc-a','pod-a'))['uid'],'uid-2')
  self.assertEqual(one(self.db,'SELECT state FROM service_members WHERE service=? AND pod=?',('svc-a','pod-a'))['state'],'recovering')
  for x in p[13:18]:run(self.db,x['command'],x['input'])
  self.assertEqual(one(self.db,'SELECT state FROM service_members WHERE service=? AND pod=?',('svc-a','pod-a'))['state'],'active')
 def test_duplicate_business_event_is_idempotent_and_conflict_is_rejected(self):
  x=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))[0]
  self.assertEqual(run(self.db,x['command'],x['input']),run(self.db,x['command'],x['input']))
  changed=dict(x['input'],uid='forged')
  self.assertEqual(run(self.db,x['command'],changed)['decision'],'event_identity_conflict')
 def test_same_failed_pod_cannot_reserve_twice_against_other_replicas(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:8]:run(self.db,x['command'],x['input'])
  for suffix in ('c','d'):
   self.step('budget','member','extra-'+suffix,service='svc-a',pod='pod-'+suffix,uid='uid-'+suffix,state='active')
  self.assertEqual(self.step('budget','credit-reserve','double',id='r2',service='svc-a',pod='pod-a',uid='uid-1',available=4,at=13)['decision'],'availability_budget_denied')
  self.assertEqual(one(self.db,'SELECT used FROM restart_credits WHERE service=?',('svc-a',))['used'],1)
 def test_healthy_probe_alone_cannot_refund_restart_credit(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:17]:run(self.db,x['command'],x['input'])
  self.assertEqual(self.step('budget','cooldown-release','premature-credit',id='r1',at=23)['decision'],'cooldown_blocked')
  self.assertEqual(one(self.db,'SELECT used FROM restart_credits WHERE service=?',('svc-a',))['used'],1)
 def test_new_probe_epoch_invalidates_old_failed_window_for_reserve(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:7]:run(self.db,x['command'],x['input'])
  self.step('probe','source-register','recreated-source',id='src',pod='pod-a',uid='uid-2',epoch=2)
  self.assertEqual(self.step('budget','credit-reserve','old-window',id='r2',service='svc-a',pod='pod-a',uid='uid-1',available=2,at=12)['decision'],'health_not_confirmed')
 def test_each_fixture_runs_from_empty_database_and_replays(self):
  expected={'probe': 'failed', 'budget': 'reserved', 'traffic': 'traffic_withdrawn'}
  for c in ('probe', 'budget', 'traffic'):
   for suffix in ('normal','failure'):
    with self.subTest(command=c,variant=suffix), tempfile.TemporaryDirectory() as td:
     path=Path(td)/'fixture.sqlite';db=connect(str(path))
     commands=json.loads((ROOT/'fixtures'/f'{c}_{suffix}.json').read_text(encoding='utf-8'))
     results=[run(db,x['command'],x['input']) for x in commands]
     db.close()
     self.assertEqual(results[-1]['decision'],expected[c] if suffix=='normal' else {'probe': 'unknown_source', 'budget': 'health_not_confirmed', 'traffic': 'traffic_still_withdrawn'}[c])
     self.assertTrue(replay(str(path))['verified'])
  self.assertEqual(Path(probecourt.__file__).resolve().parents[1],ROOT)
print('module_file',probecourt.__file__)
