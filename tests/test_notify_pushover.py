import os, sys, unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify import pushover


class PushoverTests(unittest.TestCase):
    def test_send_posts_expected_payload_and_returns_true(self):
        fake_resp = mock.Mock()
        fake_resp.raise_for_status.return_value = None
        with mock.patch.object(pushover.requests, "post", return_value=fake_resp) as post:
            ok = pushover.send("Title", "Body", priority=1, app_token="A", user_key="U")
        self.assertTrue(ok)
        args, kwargs = post.call_args
        self.assertEqual(args[0], "https://api.pushover.net/1/messages.json")
        self.assertEqual(kwargs["data"]["token"], "A")
        self.assertEqual(kwargs["data"]["user"], "U")
        self.assertEqual(kwargs["data"]["title"], "Title")
        self.assertEqual(kwargs["data"]["message"], "Body")
        self.assertEqual(kwargs["data"]["priority"], 1)

    def test_send_returns_false_when_credentials_missing(self):
        with mock.patch.object(pushover.requests, "post") as post:
            ok = pushover.send("T", "B", app_token="", user_key="")
        self.assertFalse(ok)
        post.assert_not_called()

    def test_send_returns_false_on_http_error(self):
        with mock.patch.object(pushover.requests, "post", side_effect=Exception("boom")):
            ok = pushover.send("T", "B", app_token="A", user_key="U")
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
