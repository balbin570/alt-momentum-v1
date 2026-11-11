"""Compressed, versioned backend checkpoint storage; never logs credentials."""
import base64
import gzip
import json
import os

import httpx


class CheckpointError(RuntimeError):
    pass


class SupabasePaperStore:
    def __init__(self, url, token, transport=None):
        if not url.startswith('https://') or not token:
            raise CheckpointError('STORAGE_CONFIGURATION_INVALID')
        self.url, self.token, self.revision = url, token, None
        self.transport = transport

    @classmethod
    def from_environment(cls):
        url = os.getenv('PAPER_STATE_URL', '').strip()
        token = os.getenv('PAPER_STATE_TOKEN', '').strip()
        if not url and not token:
            return None
        return cls(url, token)

    def _request(self, method, body=None):
        try:
            with httpx.Client(timeout=30., transport=self.transport) as client:
                response = client.request(method, self.url,
                    headers={'x-paper-token': self.token}, json=body)
            if response.status_code != 200:
                raise CheckpointError('CHECKPOINT_HTTP_' + str(response.status_code))
            return response.json()
        except CheckpointError:
            raise
        except Exception as exc:
            # Exception strings may include request details; return only a fixed class name.
            raise CheckpointError('CHECKPOINT_' + type(exc).__name__) from None

    def load(self):
        result = self._request('GET')
        try:
            raw = gzip.decompress(base64.b64decode(result['checkpoint_b64'], validate=True))
            payload = json.loads(raw)
            if (not isinstance(payload, dict) or not isinstance(payload.get('trades'), list)
                    or not isinstance(payload.get('seen'), dict)):
                raise ValueError('checkpoint schema')
            self.revision = int(result['revision'])
            return payload
        except Exception:
            raise CheckpointError('CHECKPOINT_DECODE_FAILED') from None

    def save(self, payload):
        if self.revision is None:
            raise CheckpointError('CHECKPOINT_NOT_LOADED')
        raw = json.dumps(payload, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        encoded = base64.b64encode(gzip.compress(raw, mtime=0)).decode()
        result = self._request('PUT', {'expected_revision': self.revision, 'checkpoint_b64': encoded})
        if int(result['revision']) != self.revision + 1:
            raise CheckpointError('CHECKPOINT_REVISION_INVALID')
        self.revision = int(result['revision'])
        return {'revision': self.revision, 'compressed_bytes': len(encoded)}
