import unittest
import os
import sys
import types

os.environ.setdefault("APP_WORKER_INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

if "openai" not in sys.modules:
    openai_stub = types.ModuleType("openai")

    class OpenAI:
        def __init__(self, *args, **kwargs) -> None:
            self.responses = types.SimpleNamespace(create=lambda **_: None)

    openai_stub.OpenAI = OpenAI
    sys.modules["openai"] = openai_stub

from app.openai_client import JobPostingOpenAiWorker


class JobPostingOpenAiWorkerTests(unittest.TestCase):
    def test_normalize_image_urls_merges_legacy_and_list(self):
        worker = JobPostingOpenAiWorker.__new__(JobPostingOpenAiWorker)

        result = worker._normalize_image_urls(
            " https://example.com/first.png ",
            [
                "https://example.com/first.png",
                "https://example.com/second.jpg",
                "https://example.com/third.png",
            ],
        )

        self.assertEqual(
            result,
            [
                "https://example.com/first.png",
                "https://example.com/second.jpg",
            ],
        )


if __name__ == "__main__":
    unittest.main()
