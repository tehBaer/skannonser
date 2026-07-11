import os, sys, unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify import send as send_mod


class SendTests(unittest.TestCase):
    def test_shells_out_to_notify_send(self):
        with mock.patch.object(send_mod.subprocess, "run") as run:
            run.return_value.returncode = 0
            ok = send_mod.send("Title", "Body", 1)
        self.assertTrue(ok)
        self.assertEqual(run.call_args[0][0],
                         ["notify", "send", "Title", "Body", "--priority", "1"])

    def test_returns_false_on_nonzero(self):
        with mock.patch.object(send_mod.subprocess, "run") as run:
            run.return_value.returncode = 1
            self.assertFalse(send_mod.send("T", "B"))

    def test_returns_false_on_exception(self):
        with mock.patch.object(send_mod.subprocess, "run", side_effect=OSError("no notify")):
            self.assertFalse(send_mod.send("T", "B"))

    def test_respects_NOTIFY_BIN_env(self):
        with mock.patch.dict(os.environ, {"NOTIFY_BIN": "/opt/notify"}), \
             mock.patch.object(send_mod.subprocess, "run") as run:
            run.return_value.returncode = 0
            send_mod.send("T", "B")
        self.assertEqual(run.call_args[0][0][0], "/opt/notify")


if __name__ == "__main__":
    unittest.main()
