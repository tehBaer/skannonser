"""Delegate a notification to the standalone `notify` CLI."""
import os
import subprocess


def send(title, message, priority=0) -> bool:
    binary = os.environ.get("NOTIFY_BIN", "notify")
    try:
        return subprocess.run(
            [binary, "send", title, message, "--priority", str(priority)],
            timeout=15,
        ).returncode == 0
    except Exception:
        return False
