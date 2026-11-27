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

 # --- late restart settlement: timing evidence for a reserved decision ---
 def _drained_prefix(self, reevaluate=False):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  for x in p[:12]:run(self.db,x['command'],x['input'])
  if reevaluate:
   run(self.db,'probe',{'event_id':'p4b','op':'window-evaluate','source':'src'})
  return p
 def _windows(self):
  return [
   self.step('budget','execution-window','w1',id='w1',restart='r1',drain='d1',uid='uid-1',start=9,finish=15),
   self.step('budget','execution-window','w2',id='w2',restart='r1',drain='d1',uid='uid-1',start=18,finish=24),
  ]
 def _late_receipt(self,tag,**kw):
  base=dict(id='r1',old_uid='uid-1',new_uid='uid-2',receipt='runtime-'+tag,sender='runtime-console',start=10,finish=12)
  base.update(kw)
  return self.step('budget','restart-settle',tag,**base)
 def test_legacy_settle_still_works_and_reevaluated_revision_rejects_plain_receipt(self):
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  with tempfile.TemporaryDirectory() as td:
   other=connect(str(Path(td)/'other.sqlite'))
   for x in p[:12]:run(other,x['command'],x['input'])
   self.assertEqual(run(other,'budget',p[12]['input'])['decision'],'restart_settled')
   other.close()
  self._drained_prefix(reevaluate=True)
  self.assertEqual(run(self.db,'budget',p[12]['input'])['decision'],'stale_restart_receipt')
  self.assertEqual(one(self.db,"SELECT status FROM restart_decisions WHERE id='r1'")['status'],'reserved')
 def test_late_receipt_settles_when_every_signed_offset_has_a_covering_window(self):
  # offsets 0 and 9 are two possible worlds; no single window holds the envelope
  self._drained_prefix(reevaluate=True)
  self.assertEqual([r['decision'] for r in self._windows()],['execution_window_registered']*2)
  r=self._late_receipt('late',offset_candidates=[0,9])
  self.assertEqual(r['decision'],'restart_settled');self.assertEqual(sorted(r['offsets']),[0,9])
  self.assertEqual(one(self.db,"SELECT uid,state FROM service_members WHERE service='svc-a' AND pod='pod-a'"),{'uid':'uid-2','state':'recovering'})
  self.assertTrue(replay(str(self.path))['verified'])
 def test_pointwise_hits_without_one_window_covering_the_whole_interval_are_rejected(self):
  self._drained_prefix(reevaluate=True);self._windows()
  r=self._late_receipt('split',start=14,finish=19,offset_candidates=[0])
  self.assertEqual(r['decision'],'review_pending')
  blocked=r['blocked'][0];self.assertEqual(blocked['offset'],0);self.assertEqual(blocked['interval'],[14,19])
  self.assertEqual(one(self.db,"SELECT status FROM restart_decisions WHERE id='r1'")['status'],'reserved')
  self.assertEqual(one(self.db,"SELECT uid FROM service_members WHERE service='svc-a' AND pod='pod-a'")['uid'],'uid-1')
 def test_unified_envelope_bucket_must_not_be_used(self):
  # same contract as the settling case: union envelope [10,21] would wrongly reject
  self._drained_prefix(reevaluate=True);self._windows()
  r=self._late_receipt('envelope',offset_candidates=[0,9])
  self.assertEqual(r['decision'],'restart_settled')
 def test_narrow_mirror_review_approves_and_wide_double_signed_review_denies(self):
  self._drained_prefix(reevaluate=True);self._windows()
  park=self._late_receipt('park',offset_candidates=[0,5,9])
  self.assertEqual(park['decision'],'review_pending')
  narrow=self.step('budget','review-fetch','rv-narrow',restart='r1',sender='runtime-calibration-mirror',offset_candidates=[0])
  self.assertEqual(narrow['decision'],'review_approved')
  self.assertEqual(one(self.db,"SELECT uid,state FROM service_members WHERE service='svc-a' AND pod='pod-a'"),{'uid':'uid-2','state':'recovering'})
  # wide paper path in a fresh court: offset 5 remains an uncovered actual world
  with tempfile.TemporaryDirectory() as td:
   wide_db=connect(str(Path(td)/'wide.sqlite'))
   wide_run=lambda c,x:run(wide_db,c,x)
   q=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
   for x in q[:12]:wide_run(x['command'],x['input'])
   wide_run('probe',{'event_id':'p4b','op':'window-evaluate','source':'src'})
   for ew in (9,18):
    wide_run('budget',{'event_id':f'w{ew}','op':'execution-window','id':f'w{ew}','restart':'r1','drain':'d1','uid':'uid-1','start':ew,'finish':ew+6})
   wide_run('budget',{'event_id':'park2','op':'restart-settle','id':'r1','old_uid':'uid-1','new_uid':'uid-2','receipt':'runtime-park2','sender':'runtime-console','start':10,'finish':12,'offset_candidates':[0,5,9]})
   wide=wide_run('budget',{'event_id':'rv-wide','op':'review-fetch','restart':'r1','sender':'double-signed-paper','offset_candidates':[0,5]})
   self.assertEqual(wide['decision'],'review_denied')
   self.assertEqual([b['offset'] for b in wide['blocked']],[5])
   self.assertEqual(one(wide_db,"SELECT status FROM restart_decisions WHERE id='r1'")['status'],'reserved')
   self.assertEqual(one(wide_db,"SELECT uid FROM service_members WHERE service='svc-a' AND pod='pod-a'")['uid'],'uid-1')
   wide_db.close()
 def test_one_review_intake_only_resends_cannot_post_twice(self):
  self._drained_prefix(reevaluate=True);self._windows()
  self._late_receipt('park3',offset_candidates=[0,5,9])
  self.step('budget','review-fetch','rv-one',restart='r1',sender='runtime-calibration-mirror',offset_candidates=[0])
  self.assertEqual(self.step('budget','review-fetch','rv-two',restart='r1',sender='double-signed-paper',offset_candidates=[0,5])['decision'],'review_closed')
  self.assertEqual(self.step('budget','clock-calibration','late-cal',id='late-cal',restart='r1',uid='uid-1',sender='paper',offset_candidates=[0])['decision'],'review_closed')
 def test_calibration_holes_intersect_and_conflicting_calibration_cannot_post(self):
  self._drained_prefix(reevaluate=True);self._windows()
  self.assertEqual(self.step('budget','clock-calibration','c1',id='c1',restart='r1',uid='uid-1',sender='hist-1',offset_candidates=[0,5,9])['decision'],'calibration_recorded')
  self.assertEqual(self.step('budget','clock-calibration','c2',id='c2',restart='r1',uid='uid-1',sender='hist-2',offset_candidates=[0,9])['decision'],'calibration_recorded')
  self.assertEqual(self._late_receipt('hole',offset_candidates=[0,5])['decision'],'restart_settled')
  # once settled the reservation is closed: further calibration originals cannot reopen the books
  self.assertEqual(self.step('budget','clock-calibration','c3',id='c3',restart='r1',uid='uid-1',sender='hist-3',offset_candidates=[5])['decision'],'stale_restart_receipt')
 def test_calibration_with_empty_intersection_is_refused(self):
  self._drained_prefix(reevaluate=True);self._windows()
  self.step('budget','clock-calibration','d1',id='d1',restart='r1',uid='uid-1',sender='hist-1',offset_candidates=[0])
  self.assertEqual(self.step('budget','clock-calibration','d2',id='d2',restart='r1',uid='uid-1',sender='hist-2',offset_candidates=[9])['decision'],'calibration_conflict')
  self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM clock_calibrations')['n'],1)
 def test_execution_window_requires_a_drained_case_and_two_window_cap(self):
  self._drained_prefix(reevaluate=False)
  # open a second court where d1 is still pending
  with tempfile.TemporaryDirectory() as td:
   early=connect(str(Path(td)/'early.sqlite'))
   q=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
   for x in q[:9]:run(early,x['command'],x['input'])
   self.assertEqual(run(early,'budget',{'event_id':'ew','op':'execution-window','id':'ew','restart':'r1','drain':'d1','uid':'uid-1','start':9,'finish':15})['decision'],'stale_restart_receipt')
   early.close()
  self.assertEqual([r['decision'] for r in self._windows()],['execution_window_registered']*2)
  self.assertEqual(self.step('budget','execution-window','w3',id='w3',restart='r1',drain='d1',uid='uid-1',start=30,finish=40)['decision'],'execution_window_limit')
 def test_no_registered_window_parks_with_blocking_evidence(self):
  self._drained_prefix(reevaluate=True)
  r=self._late_receipt('no-window',offset_candidates=[0,9])
  self.assertEqual(r['decision'],'review_pending');self.assertEqual({b['offset'] for b in r['blocked']},{0,9})
  self.assertEqual(one(self.db,"SELECT status FROM restart_receipt_reviews WHERE restart_id='r1'")['status'],'pending')
 def test_malformed_timed_receipt_is_stale_and_legacy_fields_keep_old_path(self):
  self._drained_prefix(reevaluate=True);self._windows()
  self.assertEqual(self._late_receipt('bad-clock',start=12,finish=10)['decision'],'stale_restart_receipt')
  self.assertEqual(self._late_receipt('no-sender',sender='')['decision'],'stale_restart_receipt')
  p=json.loads((ROOT/'fixtures/path.json').read_text(encoding='utf-8'))
  # a no-new-fields original still uses the exact legacy revision gate
  self.assertEqual(run(self.db,'budget',p[12]['input'])['decision'],'stale_restart_receipt')
 def test_receipt_cannot_borrow_windows_registered_for_another_reservation(self):
  self._drained_prefix(reevaluate=True);self._windows()
  # second service/decision with its own drained case must not reuse r1's windows
  self.step('budget','policy','kb-policy',service='svc-b',min_available=1,capacity=2,cooldown=10)
  self.step('budget','member','kb-member',service='svc-b',pod='pod-c',uid='uid-c',state='active')
  self.step('budget','member','kb-member2',service='svc-b',pod='pod-d',uid='uid-d',state='active')
  self.step('probe','source-register','kb-src1',id='src-c',pod='pod-c',uid='uid-c',epoch=1)
  self.step('probe','source-register','kb-src2',id='src-c2',pod='pod-c',uid='uid-c',epoch=1)
  self.step('probe','sample-record','kb-s1',id='c1',source='src-c',epoch=1,seq=1,healthy=False,at=10)
  self.step('probe','sample-record','kb-s2',id='c2',source='src-c',epoch=1,seq=2,healthy=False,at=11)
  self.step('probe','sample-record','kb-s3',id='c3',source='src-c2',epoch=1,seq=1,healthy=False,at=10)
  self.step('probe','sample-record','kb-s4',id='c4',source='src-c2',epoch=1,seq=2,healthy=False,at=11)
  self.step('probe','window-evaluate','kb-win',source='src-c')
  self.step('budget','credit-reserve','kb-r2',id='r2',service='svc-b',pod='pod-c',uid='uid-c',at=12)
  self.step('traffic','session-drain','kb-dc',id='dc',restart='r2',uid='uid-c',sessions=['s'])
  self.step('traffic','session-ack','kb-ac',case='dc',uid='uid-c',session='s')
  r=self.step('budget','restart-settle','late-c',id='r2',old_uid='uid-c',new_uid='uid-c2',receipt='rc',sender='runtime-console',start=10,finish=12,offset_candidates=[0])
  self.assertEqual(r['decision'],'review_pending');self.assertEqual(r['blocked'][0]['windows'],[])
 def test_late_restart_fixtures_replay_and_keep_business_postings(self):
  for name,tail in (('late_restart_normal','restart_settled'),('late_restart_review','review_approved')):
   with self.subTest(fixture=name),tempfile.TemporaryDirectory() as td:
    path=Path(td)/'f.sqlite';db=connect(str(path))
    results=[run(db,x['command'],x['input']) for x in json.loads((ROOT/f'fixtures/{name}.json').read_text(encoding='utf-8'))]
    self.assertIn(tail,[r['decision'] for r in results])
    self.assertEqual(results[-1]['decision'],'credit_restored')
    db.close();self.assertTrue(replay(str(path))['verified'])
print('module_file',probecourt.__file__)
