import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from probecourt.store import connect, one, replay, run

ROOT = Path(__file__).resolve().parents[1]


class VolumeReadinessTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / 'volumes.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        fixture = Path(__file__).resolve().parents[1] / 'fixtures' / 'path.json'
        steps = json.loads(fixture.read_text(encoding='utf-8'))
        for step in steps[:17]:
            run(self.db, step['command'], step['input'])
        self.restore = steps[17]['input']
        self.volumes('requirements-register', 'register-volumes', service='svc-a', pod='pod-a', volumes=['data', 'ledger'])
        for name in ('data', 'ledger'):
            self.volumes('snapshot-record', 'record-' + name, id='snap-' + name, service='svc-a', pod='pod-a',
                         volume=name, source_uid='uid-1', checkpoint=5, digest='a' * 64)

    def volumes(self, op, event_id, **kwargs):
        return run(self.db, 'volumes', {'op': op, 'event_id': event_id, **kwargs})['decision']

    def verify(self, volume, uid, digest, event_id):
        return self.volumes('snapshot-verify', event_id, volume=volume, snapshot='snap-' + volume,
                            case='d1', uid=uid, observed_digest=digest)

    def test_healthy_container_waits_for_both_mounted_volumes(self):
        self.assertEqual(run(self.db, 'traffic', dict(self.restore, event_id='early'))['decision'], 'volumes_not_verified')
        self.assertEqual(self.verify('data', 'uid-1', 'a' * 64, 'wrong-uid'), 'wrong_volume_recovery')
        self.assertEqual(self.verify('data', 'uid-2', '1' * 64, 'wrong-digest'), 'volume_digest_mismatch')
        self.assertEqual(self.verify('data', 'uid-2', 'a' * 64, 'verify-data'), 'volume_verified')
        self.assertEqual(run(self.db, 'traffic', dict(self.restore, event_id='partial'))['decision'], 'volumes_not_verified')
        self.assertEqual(self.verify('ledger', 'uid-2', 'a' * 64, 'verify-ledger'), 'volume_verified')
        self.assertEqual(run(self.db, 'traffic', self.restore)['decision'], 'traffic_restored')
        self.assertTrue(replay(self.path)['verified'])


class RecoveryPlanTests(unittest.TestCase):
    """Cross-volume transaction manifest and recovery plans on data/ledger/index."""

    PLAN_A = {'data': 'snap-data-8', 'ledger': 'snap-ledger-13', 'index': 'snap-index-7'}
    PLAN_B = {'data': 'snap-data-12', 'ledger': 'snap-ledger-9', 'index': 'snap-index-5'}

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / 'volumes.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        steps = json.loads((ROOT / 'fixtures' / 'path.json').read_text(encoding='utf-8'))
        for step in steps[:17]:
            run(self.db, step['command'], step['input'])
        self.restore = steps[17]['input']
        self.volumes('requirements-register', 'register-volumes', service='svc-a', pod='pod-a',
                     volumes=['data', 'ledger', 'index'])
        self.volumes('manifest-record', 'record-manifest', service='svc-a', pod='pod-a', transactions=[
            {'id': 'P', 'volumes': ['data', 'ledger', 'index'], 'acked': True},
            {'id': 'X', 'volumes': ['data', 'ledger', 'index'], 'acked': False},
            {'id': 'Y', 'volumes': ['data', 'ledger', 'index'], 'acked': False},
        ])
        layout = [
            ('snap-data-0', 'data', 0, '0', ['P']),
            ('snap-data-8', 'data', 8, '8', ['P', 'X']),
            ('snap-data-12', 'data', 12, 'c', ['P', 'Y']),
            ('snap-ledger-0', 'ledger', 0, '0', ['P']),
            ('snap-ledger-9', 'ledger', 9, '9', ['P', 'Y']),
            ('snap-ledger-13', 'ledger', 13, 'd', ['P', 'X']),
            ('snap-index-0', 'index', 0, '0', ['P']),
            ('snap-index-5', 'index', 5, '5', ['P', 'Y']),
            ('snap-index-7', 'index', 7, '5', ['P', 'X']),
        ]
        for snap, volume, checkpoint, digest, txns in layout:
            self.record(snap, volume, checkpoint, digest * 64, txns)

    def volumes(self, op, event_id, **kwargs):
        return run(self.db, 'volumes', {'op': op, 'event_id': event_id, **kwargs})

    def record(self, snap, volume, checkpoint, digest, txns):
        return self.volumes('snapshot-record', 'record-' + snap, id=snap, service='svc-a', pod='pod-a',
                            volume=volume, source_uid='uid-1', checkpoint=checkpoint, digest=digest,
                            transactions=txns)['decision']

    def propose(self, event_id, plan_id, selections):
        return self.volumes('plan-propose', event_id, id=plan_id, case='d1', uid='uid-2', selections=selections)

    def verify(self, event_id, volume, snapshot, digest):
        return self.volumes('snapshot-verify', event_id, volume=volume, snapshot=snapshot,
                            case='d1', uid='uid-2', observed_digest=digest)['decision']

    def restore_traffic(self, event_id, plan=None):
        x = dict(self.restore, event_id=event_id)
        if plan is not None:
            x['plan'] = plan
        return run(self.db, 'traffic', x)['decision']

    def test_plan_binds_restore_to_the_selected_snapshots(self):
        self.assertEqual(self.restore_traffic('early'), 'recovery_plan_required')
        self.assertEqual(self.verify('verify-before-plan', 'data', 'snap-data-8', '8' * 64), 'recovery_plan_required')
        base = {'data': 'snap-data-0', 'ledger': 'snap-ledger-0', 'index': 'snap-index-0'}
        self.assertEqual(self.propose('propose-base', 'plan-base', base)['decision'], 'plan_dominated')
        latest = {'data': 'snap-data-12', 'ledger': 'snap-ledger-13', 'index': 'snap-index-7'}
        self.assertEqual(self.propose('propose-latest', 'plan-latest', latest)['decision'], 'plan_inconsistent')
        self.assertEqual(self.record('snap-data-8b', 'data', 8, 'b' * 64, ['P']), 'snapshot_recorded')
        approved = self.propose('propose-a', 'plan-a', self.PLAN_A)
        self.assertEqual(approved, {'decision': 'plan_approved', 'preserved': ['P', 'X']})
        self.assertEqual(self.propose('repropose-a', 'plan-a', self.PLAN_A), approved)
        self.assertEqual(self.propose('propose-b', 'plan-b', self.PLAN_B)['decision'], 'plan_already_approved')
        self.assertEqual(self.verify('verify-data-other-branch', 'data', 'snap-data-8b', 'b' * 64),
                         'snapshot_not_in_plan')
        self.assertEqual(self.verify('verify-index-other-plan', 'index', 'snap-index-5', '5' * 64),
                         'snapshot_not_in_plan')
        self.assertEqual(self.verify('verify-data', 'data', 'snap-data-8', '8' * 64), 'volume_verified')
        self.assertEqual(self.restore_traffic('partial', plan='plan-a'), 'volumes_not_verified')
        self.assertEqual(self.verify('verify-ledger', 'ledger', 'snap-ledger-13', 'd' * 64), 'volume_verified')
        self.assertEqual(self.verify('verify-index', 'index', 'snap-index-7', '5' * 64), 'volume_verified')
        self.assertEqual(self.restore_traffic('no-plan'), 'recovery_plan_required')
        self.assertEqual(self.restore_traffic('bogus-plan', plan='plan-bogus'), 'recovery_plan_not_approved')
        self.assertEqual(self.restore_traffic('final', plan='plan-a'), 'traffic_restored')
        decision = one(self.db, 'SELECT * FROM traffic_decisions WHERE id=?', ('tr2',))
        self.assertEqual(decision['plan'], 'plan-a')
        self.assertTrue(replay(self.path)['verified'])

    def test_alternative_plan_preserves_the_other_staging_transaction(self):
        approved = self.propose('propose-b', 'plan-b', self.PLAN_B)
        self.assertEqual(approved, {'decision': 'plan_approved', 'preserved': ['P', 'Y']})
        self.assertEqual(self.verify('verify-data', 'data', 'snap-data-12', 'c' * 64), 'volume_verified')
        self.assertEqual(self.verify('verify-ledger', 'ledger', 'snap-ledger-9', '9' * 64), 'volume_verified')
        self.assertEqual(self.verify('verify-index', 'index', 'snap-index-5', '5' * 64), 'volume_verified')
        self.assertEqual(self.restore_traffic('final', plan='plan-b'), 'traffic_restored')
        self.assertTrue(replay(self.path)['verified'])

    def test_committed_transaction_cannot_be_dropped_after_snapshot_loss(self):
        self.assertEqual(self.record('snap-data-3', 'data', 3, '3' * 64, ['X']), 'snapshot_recorded')
        self.assertEqual(self.record('snap-ledger-4', 'ledger', 4, '4' * 64, ['X']), 'snapshot_recorded')
        self.assertEqual(self.record('snap-index-2', 'index', 2, '2' * 64, ['X']), 'snapshot_recorded')
        for snap in ('snap-data-0', 'snap-data-8', 'snap-data-12'):
            self.assertEqual(self.volumes('snapshot-withdraw', 'withdraw-' + snap, id=snap)['decision'],
                             'snapshot_withdrawn')
        self.assertEqual(self.volumes('snapshot-withdraw', 'withdraw-missing', id='snap-data-9')['decision'],
                         'unknown_snapshot')
        withdrawn = {'data': 'snap-data-8', 'ledger': 'snap-ledger-13', 'index': 'snap-index-7'}
        self.assertEqual(self.propose('propose-withdrawn', 'plan-w1', withdrawn)['decision'], 'invalid_plan')
        torn = {'data': 'snap-data-3', 'ledger': 'snap-ledger-13', 'index': 'snap-index-7'}
        self.assertEqual(self.propose('propose-torn', 'plan-w2', torn)['decision'], 'plan_inconsistent')
        dropping = {'data': 'snap-data-3', 'ledger': 'snap-ledger-4', 'index': 'snap-index-2'}
        self.assertEqual(self.propose('propose-drop-p', 'plan-w3', dropping)['decision'], 'plan_drops_committed')
        self.assertEqual(self.restore_traffic('no-plan'), 'recovery_plan_required')
        self.assertEqual(self.restore_traffic('rejected-plan', plan='plan-w3'), 'recovery_plan_not_approved')
        self.assertEqual(one(self.db, 'SELECT status FROM drain_cases WHERE id=?', ('d1',))['status'], 'drained')
        self.assertTrue(replay(self.path)['verified'])

    def test_manifest_and_snapshot_validation(self):
        self.assertEqual(self.volumes('requirements-register', 'too-many', service='svc-b', pod='pod-b',
                                      volumes=['a', 'b', 'c', 'd', 'e'])['decision'], 'invalid_volume_requirements')
        self.assertEqual(self.volumes('manifest-record', 'manifest-unknown-volume', service='svc-a', pod='pod-a',
                                      transactions=[{'id': 'Z', 'volumes': ['data', 'cache'], 'acked': False}])
                         ['decision'], 'invalid_manifest')
        self.assertEqual(self.volumes('manifest-record', 'manifest-duplicate', service='svc-a', pod='pod-a',
                                      transactions=[{'id': 'Z', 'volumes': ['data'], 'acked': False},
                                                    {'id': 'Z', 'volumes': ['data'], 'acked': False}])
                         ['decision'], 'invalid_manifest')
        self.assertEqual(self.record('snap-data-bogus', 'data', 20, 'e' * 64, ['W']), 'snapshot_unknown_transaction')
        self.assertEqual(self.record('snap-data-mislaid', 'data', 21, 'f' * 64, ['P', 'Q']),
                         'snapshot_unknown_transaction')
        self.assertEqual(self.record('snap-data-1', 'data', 1, '1' * 64, ['P']), 'snapshot_recorded')
        self.assertEqual(self.record('snap-data-overflow', 'data', 2, '2' * 64, ['P']), 'snapshot_limit_exceeded')
        manifest = {'service': 'svc-a', 'pod': 'pod-a', 'transactions': [
            {'id': 'P', 'volumes': ['data', 'ledger', 'index'], 'acked': True},
            {'id': 'X', 'volumes': ['data', 'ledger', 'index'], 'acked': False},
            {'id': 'Y', 'volumes': ['data', 'ledger', 'index'], 'acked': False}]}
        self.assertEqual(self.volumes('manifest-record', 'manifest-again', **manifest)['decision'], 'manifest_recorded')
        changed = dict(manifest, transactions=manifest['transactions'][:-1])
        self.assertEqual(self.volumes('manifest-record', 'manifest-changed', **changed)['decision'], 'manifest_conflict')
        self.assertTrue(replay(self.path)['verified'])


class VolumeFixtureTests(unittest.TestCase):
    def test_volume_fixtures_run_from_empty_database_and_replay(self):
        expected = {'volumes_normal.json': 'traffic_restored',
                    'volumes_plan.json': 'traffic_restored',
                    'volumes_withdrawn.json': 'recovery_plan_required'}
        for fixture, last in expected.items():
            with self.subTest(fixture=fixture), TemporaryDirectory() as td:
                path = Path(td) / 'fixture.sqlite'
                db = connect(str(path))
                commands = json.loads((ROOT / 'fixtures' / fixture).read_text(encoding='utf-8'))
                results = [run(db, x['command'], x['input']) for x in commands]
                db.close()
                self.assertEqual(results[-1]['decision'], last)
                self.assertTrue(replay(str(path))['verified'])

    def test_plan_fixture_approves_plan_preserving_committed_and_one_staging(self):
        with TemporaryDirectory() as td:
            db = connect(str(Path(td) / 'fixture.sqlite'))
            commands = json.loads((ROOT / 'fixtures' / 'volumes_plan.json').read_text(encoding='utf-8'))
            results = [run(db, x['command'], x['input']) for x in commands]
            db.close()
        approved = [result for result in results if result['decision'] == 'plan_approved']
        self.assertEqual(approved, [{'decision': 'plan_approved', 'preserved': ['P', 'X']}])
        self.assertIn('plan_dominated', [result['decision'] for result in results])
        self.assertIn('plan_inconsistent', [result['decision'] for result in results])
        self.assertIn('snapshot_not_in_plan', [result['decision'] for result in results])
