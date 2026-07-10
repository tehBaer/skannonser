"""Single choke-point for sending Pushover notifications."""
import requests

from main.notify import config

API_URL = "https://api.pushover.net/1/messages.json"


def send(title, message, priority=0, *, app_token=None, user_key=None, timeout=10) -> bool:
    token = app_token if app_token is not None else config.PUSHOVER_APP_TOKEN
    user = user_key if user_key is not None else config.PUSHOVER_USER_KEY
    if not token or not user:
        print("[pushover] missing credentials; not sending")
        return False
    payload = {
        "token": token,
        "user": user,
        "title": title,
        "message": message,
        "priority": priority,
    }
    try:
        resp = requests.post(API_URL, data=payload, timeout=timeout)
        resp.raise_for_status()
        return True
    except Exception as exc:
        print(f"[pushover] send failed: {exc}")
        return False
