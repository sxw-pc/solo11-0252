import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from probecourt.store import connect, replay, run


class HandoffTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / 'handoff.sqlite')
        self.db = connect(self.path)
        self.addCleanup(self.db.close)
        fixture = Path(__file__).resolve().parents[1] / 'fixtures' / 'path.json'
        self.steps = json.loads(fixture.read_text(encoding='utf-8'))
        for step in self.steps[:9]:
            run(self.db, step['command'], step['input'])

    def traffic(self, op, event_id, **kwargs):
        return run(self.db, 'traffic', dict(op=op, event_id=event_id, case='d1', uid='uid-1', **kwargs))['decision']

    def manifest(self, second_gateway):
        return self.traffic('handoff-manifest', 'plan', gateways={'A': {'uid': 'A1', 'capacity': 2},
            'B': {'uid': 'B1', 'capacity': 2}}, assignments={
            'session-a': {'gateway': 'A', 'units': 2},
            'session-b': {'gateway': second_gateway, 'units': 1},
        })

    def adopt(self, suffix, gateway, gateway_uid, event_id):
        return self.traffic('session-adopt', event_id, session='session-' + suffix, manifest='plan',
                            gateway=gateway, gateway_uid=gateway_uid, receipt='receiver-' + suffix)

    def test_receipts_gate_drain_and_restart(self):
        self.assertEqual(self.manifest('B'), 'handoff_placed')
        self.assertEqual(self.traffic('session-ack', 'plain-ack', session='session-a'), 'handoff_receipt_required')
        self.assertEqual(self.adopt('a', 'A', 'A0', 'stale'), 'handoff_receipt_mismatch')
        self.assertEqual(self.adopt('a', 'A', 'A1', 'a-adopts'), 'partially_drained')
        settlement = self.steps[12]['input']
        self.assertEqual(run(self.db, 'budget', dict(settlement, event_id='early'))['decision'], 'stale_restart_receipt')
        self.assertEqual(self.adopt('b', 'B', 'B1', 'b-adopts'), 'drained')
        self.assertEqual(run(self.db, 'budget', settlement)['decision'], 'restart_settled')
        self.assertTrue(replay(self.path)['verified'])

    def test_gateway_limit_applies_to_sum_of_sessions(self):
        self.assertEqual(self.manifest('A'), 'handoff_over_capacity')
        self.assertEqual(self.adopt('a', 'A', 'A1', 'a-adopts'), 'handoff_receipt_mismatch')
        self.assertTrue(replay(self.path)['verified'])
