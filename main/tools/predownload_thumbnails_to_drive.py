#!/usr/bin/env python3
"""Pre-download listing thumbnails and host them on Google Drive.

This avoids browser hotlink/cross-origin issues for FINN-hosted images by serving
stable, controlled URLs from a Drive folder.
"""

import argparse
import io
import os
import sqlite3
import sys
import traceback
from typing import Optional

import requests
from PIL import Image
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.database.db import PropertyDatabase
    from main.googleUtils import get_service_account_credentials
except ImportError:
    from database.db import PropertyDatabase
    from googleUtils import get_service_account_credentials

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.metadata",
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "main", "config")
DRIVE_TOKEN_PATH = os.path.join(CONFIG_DIR, "drive_token.json")
CREDENTIALS_PATH = os.path.join(CONFIG_DIR, "credentials.json")


def get_drive_credentials(auth_mode: str = "auto") -> Credentials:
    mode = (auth_mode or "auto").strip().lower()
    if mode not in {"auto", "service-account", "oauth"}:
        raise ValueError(f"Unsupported --auth-mode: {auth_mode}")

    # Prefer service-account auth for unattended runs unless explicitly set to oauth.
    if mode in {"auto", "service-account"}:
        service_account_creds = get_service_account_credentials(DRIVE_SCOPES)
        if service_account_creds is not None:
            return service_account_creds
        if mode == "service-account":
            raise RuntimeError("Service-account mode requested but no service-account key file was found")

    creds = None
    if os.path.exists(DRIVE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(DRIVE_TOKEN_PATH, DRIVE_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, DRIVE_SCOPES)
            creds = flow.run_local_server(port=0)

        with open(DRIVE_TOKEN_PATH, "w", encoding="utf-8") as token_file:
            token_file.write(creds.to_json())

    return creds


def ensure_folder(service, folder_id: Optional[str], folder_name: str) -> str:
    if folder_id:
        return folder_id

    q = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{folder_name}' and trashed=false"
    )
    existing = service.files().list(q=q, fields="files(id,name)", pageSize=1).execute().get("files", [])
    if existing:
        return existing[0]["id"]

    created = service.files().create(
        body={"name": folder_name, "mimeType": "application/vnd.google-apps.folder"},
        fields="id",
    ).execute()
    return created["id"]


def find_or_create_file(service, folder_id: str, finnkode: str, content: bytes, mime_type: str) -> str:
    safe_name = f"{finnkode}.jpg"
    q = (
        f"'{folder_id}' in parents and "
        f"name='{safe_name}' and trashed=false"
    )
    found = service.files().list(q=q, fields="files(id,name)", pageSize=1).execute().get("files", [])

    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=False)

    if found:
        file_id = found[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
    else:
        created = service.files().create(
            body={"name": safe_name, "parents": [folder_id]},
            media_body=media,
            fields="id",
        ).execute()
        file_id = created["id"]

    return file_id


def ensure_public(service, file_id: str) -> None:
    perms = service.permissions().list(fileId=file_id, fields="permissions(id,type,role)").execute().get("permissions", [])
    for p in perms:
        if p.get("type") == "anyone" and p.get("role") == "reader":
            return

    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()


def to_drive_image_url(file_id: str) -> str:
    return f"https://drive.google.com/uc?export=view&id={file_id}"


def download_image(url: str, timeout: float) -> tuple[bytes, str]:
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()
    content_type = (resp.headers.get("content-type") or "image/jpeg").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        raise ValueError(f"Unexpected content-type: {content_type}")

    content = resp.content
    if not content:
        raise ValueError("Empty image body")

    return content, content_type


def to_thumbnail_jpeg(content: bytes, max_dim: int, quality: int) -> tuple[bytes, str]:
    """Downscale and recompress to a reasonable thumbnail size for map cards."""
    with Image.open(io.BytesIO(content)) as img:
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        elif img.mode == "L":
            img = img.convert("RGB")

        img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=quality, optimize=True)
        return out.getvalue(), "image/jpeg"


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-download and host listing thumbnails in Google Drive")
    parser.add_argument("--db", help="Optional path to properties.db")
    parser.add_argument("--folder-id", help="Existing Google Drive folder ID (optional)")
    parser.add_argument("--folder-name", default="Skannonser Thumbnails", help="Folder name when creating/searching folder")
    parser.add_argument("--limit", type=int, default=0, help="Maximum listings to process (0 = all)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite already hosted URLs")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout in seconds")
    parser.add_argument("--auth-mode", default="auto", choices=["auto", "service-account", "oauth"], help="Drive auth mode")
    parser.add_argument("--max-dim", type=int, default=640, help="Max width/height in pixels for uploaded thumbnails")
    parser.add_argument("--jpeg-quality", type=int, default=82, help="JPEG quality for uploaded thumbnails")
    parser.add_argument("--include-inactive", action="store_true", help="Also process listings with active != 1")
    parser.add_argument("--verbose", action="store_true", help="Print per-row failures")
    args = parser.parse_args()

    db = PropertyDatabase(args.db)
    conn = sqlite3.connect(db.db_path)
    cur = conn.cursor()

    where = "TRIM(COALESCE(image_url, '')) != ''"
    if not args.include_inactive:
        where += " AND COALESCE(active, 1) = 1"
    if not args.overwrite:
        where += " AND TRIM(COALESCE(image_hosted_url, '')) = ''"

    query = f"""
        SELECT finnkode, image_url
        FROM eiendom
        WHERE {where}
        ORDER BY scraped_at DESC
    """
    if args.limit and args.limit > 0:
        query += " LIMIT ?"
        rows = cur.execute(query, (args.limit,)).fetchall()
    else:
        rows = cur.execute(query).fetchall()

    total = len(rows)
    print(f"Candidates: {total}")
    if total == 0:
        conn.close()
        return 0

    creds = get_drive_credentials(args.auth_mode)
    drive = build("drive", "v3", credentials=creds)
    folder_id = ensure_folder(drive, args.folder_id, args.folder_name)
    print(f"Drive folder id: {folder_id}")

    updated = 0
    failed = 0
    failure_samples = []

    for idx, (finnkode, image_url) in enumerate(rows, start=1):
        fk = str(finnkode or "").strip()
        src = str(image_url or "").strip()
        if not fk or not src:
            failed += 1
            continue

        try:
            content, mime_type = download_image(src, timeout=args.timeout)
            content, mime_type = to_thumbnail_jpeg(content, max_dim=args.max_dim, quality=args.jpeg_quality)
            file_id = find_or_create_file(drive, folder_id, fk, content, mime_type)
            ensure_public(drive, file_id)
            hosted_url = to_drive_image_url(file_id)

            cur.execute(
                """
                UPDATE eiendom
                SET image_hosted_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
                """,
                (hosted_url, fk),
            )
            if cur.rowcount > 0:
                updated += 1
        except Exception as err:
            failed += 1
            if len(failure_samples) < 10:
                failure_samples.append((fk, str(err)))
            if args.verbose:
                print(f"FAIL #{fk}: {err}")
                if args.verbose:
                    traceback.print_exc(limit=1)

        if idx % 50 == 0:
            conn.commit()
            print(f"Processed {idx}/{total}, updated {updated}, failed {failed}")

    conn.commit()
    conn.close()

    print("\nThumbnail hosting complete")
    print(f"Updated IMAGE_HOSTED_URL: {updated}")
    print(f"Failed: {failed}")
    print(f"Folder ID: {folder_id}")
    if failure_samples:
        print("Failure samples:")
        for fk, msg in failure_samples:
            print(f"  #{fk}: {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
