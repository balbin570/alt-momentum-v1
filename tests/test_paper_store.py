import base64
import gzip
import json
import unittest
from unittest.mock import patch

import httpx

from paper_store import CheckpointError, SupabasePaperStore


class StoreTests(unittest.TestCase):
    payload = {'trades': [], 'seen': {}, 'signals': 0}

    def encoded(self):
        return base64.b64encode(gzip.compress(json.dumps(self.payload).encode())).decode()

    def test_load_and_compressed_compare_and_swap_save(self):
        def handler(req):
            self.assertEqual('test-secret', req.headers['x-paper-token'])
            if req.method == 'GET':
                return httpx.Response(200, json={'revision': 4, 'checkpoint_b64': self.encoded()})
            body = json.loads(req.content)
            self.assertEqual(4, body['expected_revision'])
            self.assertEqual(self.payload, json.loads(gzip.decompress(base64.b64decode(body['checkpoint_b64']))))
            return httpx.Response(200, json={'revision': 5})
        store = SupabasePaperStore('https://example.test/state', 'test-secret', httpx.MockTransport(handler))
        self.assertEqual(self.payload, store.load())
        self.assertEqual(5, store.save(self.payload)['revision'])

    def test_save_is_blocked_until_successful_restore(self):
        store = SupabasePaperStore('https://example.test/state', 'test-secret')
        with self.assertRaisesRegex(CheckpointError, 'NOT_LOADED'):
            store.save(self.payload)

    def test_conflict_does_not_advance_revision(self):
        store = SupabasePaperStore('https://example.test/state', 'test-secret',
            httpx.MockTransport(lambda r: httpx.Response(409)))
        store.revision = 4
        with self.assertRaisesRegex(CheckpointError, 'HTTP_409'):
            store.save(self.payload)
        self.assertEqual(4, store.revision)

    def test_exception_does_not_expose_sensitive_message(self):
        def fail(req):
            raise RuntimeError('test-secret sensitive request')
        store = SupabasePaperStore('https://example.test/state', 'test-secret', httpx.MockTransport(fail))
        with self.assertRaises(CheckpointError) as result:
            store.load()
        self.assertNotIn('test-secret', str(result.exception))
        self.assertEqual('CHECKPOINT_RuntimeError', str(result.exception))

    def test_bad_remote_data_does_not_become_an_empty_history(self):
        store = SupabasePaperStore('https://example.test/state', 'test-secret',
            httpx.MockTransport(lambda r: httpx.Response(200, json={'revision': 1, 'checkpoint_b64': 'broken'})))
        with self.assertRaisesRegex(CheckpointError, 'DECODE_FAILED'):
            store.load()
        self.assertIsNone(store.revision)

    def test_incomplete_configuration_is_rejected(self):
        with patch.dict('os.environ', {'PAPER_STATE_URL': 'https://example.test/state', 'PAPER_STATE_TOKEN': ''}):
            with self.assertRaises(CheckpointError):
                SupabasePaperStore.from_environment()
