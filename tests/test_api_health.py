"""GET /api/health must succeed without OPENAI_API_KEY (Cloud Run probes)."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from api.index import app


class TestApiHealth(unittest.TestCase):
    def test_health_ok_without_api_key(self):
        env = {k: v for k, v in os.environ.items() if k != "OPENAI_API_KEY"}
        with mock.patch.dict(os.environ, env, clear=True):
            client = TestClient(app)
            res = client.get("/api/health")
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("status"), "ok")
        self.assertIn("openai_api_key_configured", body)
        self.assertFalse(body["openai_api_key_configured"])


if __name__ == "__main__":
    unittest.main()
