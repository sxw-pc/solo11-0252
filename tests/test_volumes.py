import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from probecourt.store import connect, replay, run


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
