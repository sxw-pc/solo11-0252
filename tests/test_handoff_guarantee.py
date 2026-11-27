import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from probecourt.store import connect, replay, run, one

ROOT = Path(__file__).resolve().parents[1]
SESSIONS = {'s1': 1, 's2': 2, 's3': 2}
ELIGIBLE = {'s1': ['A', 'B'], 's2': ['A', 'C'], 's3': ['B', 'D']}
GATEWAYS_CAP2 = {g: {'uid': g + '1', 'capacity': 2} for g in 'ABCD'}
GATEWAYS_CAP3 = dict(GATEWAYS_CAP2)
GATEWAYS_CAP3['A'] = {'uid': 'A1', 'capacity': 3}
GATEWAYS_CAP3['B'] = {'uid': 'B1', 'capacity': 3}
PLAN_1 = {
    's1': {'primary': 'A', 'backup': 'B'},
    's2': {'primary': 'C', 'backup': 'A'},
    's3': {'primary': 'D', 'backup': 'B'},
}
PLAN_2 = {
    's1': {'primary': 'B', 'backup': 'A'},
    's2': {'primary': 'C', 'backup': 'A'},
    's3': {'primary': 'B', 'backup': 'D'},
}
SETTLE = {'event_id': 'settle', 'op': 'restart-settle', 'id': 'r1',
          'old_uid': 'uid-1', 'new_uid': 'uid-2', 'receipt': 'runtime-uid2'}


class GuaranteeTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / 'guarantee.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        prefix = json.loads((ROOT / 'fixtures/path.json').read_text(encoding='utf-8'))
        for step in prefix[:8]:
            run(self.db, step['command'], step['input'])
        run(self.db, 'traffic', dict(op='session-drain', event_id='t1', id='d1',
                                     restart='r1', uid='uid-1', sessions=['s1', 's2', 's3']))

    def traffic(self, op, event_id, **kwargs):
        return run(self.db, 'traffic',
                   dict(op=op, event_id=event_id, case='d1', uid='uid-1', **kwargs))

    def manifest(self, event_id, gateways=GATEWAYS_CAP3, plan=PLAN_1, **extra):
        return self.traffic('handoff-manifest', event_id, gateways=gateways,
                            sessions=SESSIONS, eligible=ELIGIBLE, plan=plan, **extra)

    def adopt(self, event_id, session, gateway, uid, manifest='plan', receipt=None):
        return self.traffic('session-adopt', event_id, session=session, manifest=manifest,
                            gateway=gateway, gateway_uid=uid,
                            receipt=receipt if receipt is not None else 'r-' + session)

    def assert_replays(self):
        self.assertTrue(replay(self.path)['verified'])

    def test_cap2_each_outage_packable_but_joint_commitment_rejected(self):
        result = self.traffic('handoff-manifest', 'rj', gateways=GATEWAYS_CAP2,
                              sessions=SESSIONS, eligible=ELIGIBLE)
        self.assertEqual(result['decision'], 'handoff_guarantee_unsatisfiable')
        by_offline = {entry['offline']: entry for entry in result['evidence']}
        # Every individual scenario, including each outage, has a packing.
        self.assertTrue(all(entry['feasible'] for entry in result['evidence']))
        self.assertEqual([by_offline['A']['placement'][s] for s in ('s1', 's2', 's3')],
                         ['B', 'C', 'D'])
        self.assertEqual([by_offline['B']['placement'][s] for s in ('s1', 's2', 's3')],
                         ['A', 'C', 'D'])
        self.assertEqual([by_offline['C']['placement'][s] for s in ('s1', 's2', 's3')],
                         ['B', 'A', 'D'])
        self.assertEqual([by_offline['D']['placement'][s] for s in ('s1', 's2', 's3')],
                         ['A', 'C', 'B'])
        # A rejected manifest leaves nothing to adopt against.
        self.assertEqual(self.adopt('x', 's1', 'A', 'A1', manifest='rj')['decision'],
                         'handoff_receipt_mismatch')
        self.assertIsNone(one(self.db, 'SELECT * FROM handoff_guarantee_manifests'))
        self.assert_replays()

    def test_explicit_plan_violating_a_scenario_is_rejected_with_evidence(self):
        result = self.manifest('bad', gateways=GATEWAYS_CAP2)
        self.assertEqual(result['decision'], 'handoff_plan_violates_guarantee')
        self.assertEqual(result['violation']['offline'], 'C')
        self.assertEqual(result['violation']['over_capacity'], {'A': 3})
        self.assertTrue(all('placement' in entry for entry in result['evidence']))
        self.assertIsNone(one(self.db, 'SELECT * FROM handoff_guarantee_manifests'))
        self.assert_replays()

    def test_two_cap3_plans_both_pass_all_five_scenarios(self):
        for plan in (PLAN_1, PLAN_2):
            result = self.manifest('plan', plan=plan)
            self.assertEqual(result['decision'], 'handoff_guarantee_placed')
            self.assertEqual(len(result['scenario_loads']), 5)
            for entry in result['scenario_loads']:
                for gateway, units in entry['loads'].items():
                    self.assertLessEqual(units, GATEWAYS_CAP3[gateway]['capacity'])
            # Each case needs a fresh ledger because one manifest binds the case.
            self.setUp()
        self.assert_replays()

    def test_adopts_must_match_current_effective_targets_bcd(self):
        self.assertEqual(self.manifest('plan')['decision'], 'handoff_guarantee_placed')
        offline = self.traffic('gateway-offline', 'off-a', gateway='A')
        self.assertEqual(offline['decision'], 'gateway_offline_recorded')
        self.assertEqual(offline['activated_failovers'], ['s1'])
        # Lost primary cannot clear the session any more.
        self.assertEqual(self.adopt('stale-a', 's1', 'A', 'A1')['decision'],
                         'handoff_receipt_mismatch')
        # The other plan's s3 ownership (B under BCB) is not this case's target.
        self.assertEqual(self.adopt('cross-s3', 's3', 'B', 'B1')['decision'],
                         'handoff_receipt_mismatch')
        # Forged gateway uid is rejected even with the right gateway name.
        self.assertEqual(self.adopt('forged', 's2', 'C', 'C9')['decision'],
                         'handoff_receipt_mismatch')
        # Real BCD ownership receipts clear the case.
        self.assertEqual(self.adopt('a1', 's1', 'B', 'B1')['decision'], 'partially_drained')
        self.assertEqual(self.adopt('a2', 's2', 'C', 'C1')['decision'], 'partially_drained')
        self.assertEqual(self.adopt('a3', 's3', 'D', 'D1')['decision'], 'drained')
        self.assertEqual(run(self.db, 'budget', SETTLE)['decision'], 'restart_settled')
        # Resending an old primary receipt after settlement stays invalid.
        self.assertEqual(self.adopt('stale-again', 's1', 'A', 'A1')['decision'],
                         'handoff_unavailable')
        self.assert_replays()

    def test_bcb_plan_keeps_s3_on_surviving_primary_b(self):
        self.assertEqual(self.manifest('plan', plan=PLAN_2)['decision'],
                         'handoff_guarantee_placed')
        self.assertEqual(self.traffic('gateway-offline', 'off-a', gateway='A')['decision'],
                         'gateway_offline_recorded')
        # s1 primary B survives and must not move to backup A.
        self.assertEqual(self.adopt('move-s1', 's1', 'A', 'A1')['decision'],
                         'handoff_receipt_mismatch')
        # s3 primary B survives as well; plan 1 would have put it on D.
        self.assertEqual(self.adopt('plan1-s3', 's3', 'D', 'D1')['decision'],
                         'handoff_receipt_mismatch')
        self.assertEqual(self.adopt('a1', 's1', 'B', 'B1')['decision'], 'partially_drained')
        self.assertEqual(self.adopt('a2', 's2', 'C', 'C1')['decision'], 'partially_drained')
        self.assertEqual(self.adopt('a3', 's3', 'B', 'B1')['decision'], 'drained')
        self.assertEqual(run(self.db, 'budget', SETTLE)['decision'], 'restart_settled')
        self.assert_replays()

    def test_offline_report_voids_prior_primary_receipt(self):
        self.assertEqual(self.manifest('plan')['decision'], 'handoff_guarantee_placed')
        # s1 lands on primary A while everyone is online.
        self.assertEqual(self.adopt('early', 's1', 'A', 'A1')['decision'], 'partially_drained')
        report = self.traffic('gateway-offline', 'off-a', gateway='A')
        self.assertEqual(report['decision'], 'gateway_offline_recorded')
        self.assertEqual(report['pending'], 3)
        self.assertEqual(one(self.db, "SELECT status FROM session_receipts WHERE id='d1:s1'")['status'],
                         'pending')
        self.assertIsNone(one(
            self.db,
            "SELECT receipt FROM handoff_failover_assignments WHERE case_id='d1' AND session='s1'"
        )['receipt'])
        self.assertEqual(one(self.db, "SELECT status FROM drain_cases WHERE id='d1'")['status'],
                         'pending')
        # Replaying the identical offline event returns the stored decision.
        self.assertEqual(self.traffic('gateway-offline', 'off-a', gateway='A')['decision'],
                         'gateway_offline_recorded')
        # A second offline report for the same gateway is a no-op.
        self.assertEqual(self.traffic('gateway-offline', 'off-a-again', gateway='A')['decision'],
                         'gateway_already_offline')
        # Backup B now completes the failover receipt.
        self.assertEqual(self.adopt('failover', 's1', 'B', 'B1')['decision'], 'partially_drained')
        self.assert_replays()

    def test_other_outages_only_activate_their_own_primaries(self):
        self.assertEqual(self.manifest('plan')['decision'], 'handoff_guarantee_placed')
        # D outage moves only s3 to its fixed backup B; s1 stays on A.
        report = self.traffic('gateway-offline', 'off-d', gateway='D')
        self.assertEqual(report['activated_failovers'], ['s3'])
        self.assertEqual(self.adopt('s3-moved', 's3', 'B', 'B1')['decision'], 'partially_drained')
        self.assertEqual(self.adopt('s3-old', 's3', 'D', 'D1')['decision'],
                         'handoff_receipt_mismatch')
        self.assertEqual(self.adopt('s1-stays', 's1', 'A', 'A1')['decision'], 'partially_drained')
        self.assertEqual(self.adopt('s2-stays', 's2', 'C', 'C1')['decision'], 'drained')
        self.assertEqual(run(self.db, 'budget', SETTLE)['decision'], 'restart_settled')
        self.assert_replays()

    def test_session_with_both_gateways_lost_cannot_be_adopted_anywhere(self):
        self.assertEqual(self.manifest('plan')['decision'], 'handoff_guarantee_placed')
        # s2 uses primary C with backup A. Losing both leaves it with no
        # reachable committed target, so no receipt can clear it.
        self.assertEqual(self.traffic('gateway-offline', 'off-a', gateway='A')['decision'],
                         'gateway_offline_recorded')
        second = self.traffic('gateway-offline', 'off-c', gateway='C')
        self.assertEqual(second['decision'], 'gateway_offline_recorded')
        self.assertEqual(second['orphaned_sessions'], ['s2'])
        for gateway, uid in (('A', 'A1'), ('C', 'C1'), ('B', 'B1')):
            self.assertEqual(self.adopt('s2-' + gateway, 's2', gateway, uid)['decision'],
                             'handoff_receipt_mismatch')
        self.assertEqual(one(
            self.db,
            "SELECT status FROM session_receipts WHERE id='d1:s2'")['status'], 'pending')
        self.assert_replays()

    def test_solver_selects_a_commitment_when_capacity_allows(self):
        result = self.traffic('handoff-manifest', 'auto', gateways=GATEWAYS_CAP3,
                              sessions=SESSIONS, eligible=ELIGIBLE)
        self.assertEqual(result['decision'], 'handoff_guarantee_placed')
        for sid, pair in result['plan'].items():
            self.assertIn(pair['primary'], ELIGIBLE[sid])
            self.assertIn(pair['backup'], ELIGIBLE[sid])
            self.assertNotEqual(pair['primary'], pair['backup'])
        self.assert_replays()

    def test_outage_without_any_packing_is_distinct_from_joint_failure(self):
        # When D is lost, both demand-2 sessions only survive on C (capacity 2),
        # so this outage itself has no packing even though the online layout fits.
        with TemporaryDirectory() as td:
            path = str(Path(td) / 'dead.sqlite')
            db = connect(path)
            prefix = json.loads((ROOT / 'fixtures/path.json').read_text(encoding='utf-8'))
            for step in prefix[:8]:
                run(db, step['command'], step['input'])
            run(db, 'traffic', dict(op='session-drain', event_id='t1', id='d1',
                                    restart='r1', uid='uid-1', sessions=['s2', 's3']))
            gateways = {'A': {'uid': 'A1', 'capacity': 2}, 'B': {'uid': 'B1', 'capacity': 2},
                        'C': {'uid': 'C1', 'capacity': 2}, 'D': {'uid': 'D1', 'capacity': 2}}
            result = run(db, 'traffic', dict(
                op='handoff-manifest', event_id='dead', case='d1', uid='uid-1',
                gateways=gateways, sessions={'s2': 2, 's3': 2},
                eligible={'s2': ['C', 'D'], 's3': ['C', 'D']}))
            self.assertEqual(result['decision'], 'handoff_scenario_infeasible')
            d_outage = next(entry for entry in result['evidence'] if entry['offline'] == 'D')
            self.assertFalse(d_outage['feasible'])
            self.assertEqual(d_outage['unplaced'], ['s2', 's3'])
            online = next(entry for entry in result['evidence'] if entry['offline'] is None)
            self.assertTrue(online['feasible'])
            self.assertIsNone(one(db, 'SELECT * FROM handoff_guarantee_manifests'))
            db.close()
            self.assertTrue(replay(path)['verified'])

    def test_invalid_guarantee_inputs_are_rejected(self):
        bad_gateways = dict(GATEWAYS_CAP3, E={'uid': 'E1', 'capacity': 1})
        self.assertEqual(
            self.manifest('m1', gateways=bad_gateways)['decision'], 'handoff_invalid_gateway')
        self.assertEqual(
            self.traffic('handoff-manifest', 'm2', gateways=GATEWAYS_CAP3,
                         sessions={'s1': 1, 's2': 2}, eligible=ELIGIBLE)['decision'],
            'handoff_uncovered_sessions')
        self.assertEqual(
            self.traffic('handoff-manifest', 'm3', gateways=GATEWAYS_CAP3, sessions=SESSIONS,
                         eligible={'s1': ['A'], 's2': ['A', 'C'], 's3': ['B', 'D']})['decision'],
            'handoff_invalid_eligible')
        self.assertEqual(
            self.traffic('handoff-manifest', 'm4', gateways=GATEWAYS_CAP3, sessions=SESSIONS,
                         eligible=dict(ELIGIBLE, s1=['A', 'X']))['decision'],
            'handoff_invalid_eligible')
        self.assertEqual(
            self.manifest('m5', plan=dict(PLAN_1, s1={'primary': 'A', 'backup': 'A'}))['decision'],
            'handoff_plan_invalid')
        self.assertEqual(
            self.manifest('m6', plan=dict(PLAN_1, s1={'primary': 'C', 'backup': 'B'}))['decision'],
            'handoff_plan_invalid')
        self.assertEqual(
            self.traffic('gateway-offline', 'm7', gateway='X')['decision'],
            'handoff_unavailable')
        self.assert_replays()

    def test_second_manifest_and_verbal_ack_are_refused(self):
        self.assertEqual(self.manifest('plan')['decision'], 'handoff_guarantee_placed')
        self.assertEqual(self.manifest('again')['decision'], 'handoff_unavailable')
        self.assertEqual(self.traffic('session-ack', 'verbal', session='s1')['decision'],
                         'handoff_receipt_required')
        self.assertEqual(self.traffic('gateway-offline', 'off-a', gateway='A')['decision'],
                         'gateway_offline_recorded')
        self.assertEqual(self.traffic('session-ack', 'verbal2', session='s1')['decision'],
                         'handoff_receipt_required')
        self.assert_replays()

    def test_legacy_manifest_path_is_unchanged(self):
        result = self.traffic(
            'handoff-manifest', 'legacy',
            gateways={'A': {'uid': 'A1', 'capacity': 2}, 'B': {'uid': 'B1', 'capacity': 2}},
            assignments={'s1': {'gateway': 'A', 'units': 1},
                         's2': {'gateway': 'B', 'units': 1},
                         's3': {'gateway': 'A', 'units': 1}})
        self.assertEqual(result['decision'], 'handoff_placed')
        self.assertEqual(self.adopt('l1', 's1', 'A', 'A1', manifest='legacy')['decision'],
                         'partially_drained')
        self.assertEqual(self.adopt('l2', 's2', 'B', 'B1', manifest='legacy')['decision'],
                         'partially_drained')
        self.assertEqual(self.adopt('l3', 's3', 'A', 'A1', manifest='legacy')['decision'],
                         'drained')
        self.assertEqual(run(self.db, 'budget', SETTLE)['decision'], 'restart_settled')
        self.assert_replays()

    def test_fixture_artifacts_run_and_replay(self):
        for name, expected in (
            ('handoff_guarantee_bcd.json', 'restart_settled'),
            ('handoff_guarantee_bcb.json', 'restart_settled'),
            ('handoff_guarantee_rejected.json', 'handoff_receipt_mismatch'),
            ('handoff_normal.json', 'restart_settled'),
        ):
            with self.subTest(fixture=name), TemporaryDirectory() as td:
                path = str(Path(td) / 'fx.sqlite')
                db = connect(path)
                commands = json.loads((ROOT / 'fixtures' / name).read_text(encoding='utf-8'))
                results = [run(db, item['command'], item['input']) for item in commands]
                db.close()
                self.assertEqual(results[-1]['decision'], expected)
                self.assertTrue(replay(path)['verified'])


if __name__ == '__main__':
    unittest.main()
