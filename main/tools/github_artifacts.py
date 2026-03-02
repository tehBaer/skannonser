#!/usr/bin/env python3
"""
Utility for downloading and cleaning up GitHub Actions artifacts.

Typical low-cost flow:
1) Pull artifacts locally and record what was downloaded.
2) Delete only artifacts that were confirmed downloaded.

Examples:
  python main/tools/github_artifacts.py pull \
    --repo owner/repo \
    --prefix html-delta- \
    --dest artifacts/github \
    --delete-after-download

  python main/tools/github_artifacts.py cleanup-manifest \
    --repo owner/repo \
    --manifest artifacts/github/download_manifest.json
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any

import requests

GITHUB_API = "https://api.github.com"
DEFAULT_MANIFEST = "artifacts/github/download_manifest.json"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_repo(repo: str) -> tuple[str, str]:
    if "/" not in repo:
        raise ValueError("--repo must be in 'owner/repo' format")
    owner, name = repo.split("/", 1)
    return owner.strip(), name.strip()


def github_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "skannonser-artifact-tool",
    }


def list_artifacts(owner: str, repo: str, token: str, prefix: str = "") -> List[Dict[str, Any]]:
    headers = github_headers(token)
    artifacts: List[Dict[str, Any]] = []
    page = 1

    while True:
        resp = requests.get(
            f"{GITHUB_API}/repos/{owner}/{repo}/actions/artifacts",
            headers=headers,
            params={"per_page": 100, "page": page},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        chunk = data.get("artifacts", [])
        if not chunk:
            break

        for artifact in chunk:
            if artifact.get("expired"):
                continue
            name = artifact.get("name", "")
            if prefix and not name.startswith(prefix):
                continue
            artifacts.append(artifact)

        if len(chunk) < 100:
            break
        page += 1

    # oldest first, so cleanup/downloading is deterministic
    artifacts.sort(key=lambda item: item.get("created_at", ""))
    return artifacts


def sanitize_filename(name: str) -> str:
    keep = "-_.() "
    return "".join(ch for ch in name if ch.isalnum() or ch in keep).strip().replace(" ", "_")


def download_artifact_zip(owner: str, repo: str, token: str, artifact_id: int, output_path: Path) -> None:
    headers = github_headers(token)
    url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip"
    with requests.get(url, headers=headers, stream=True, timeout=300, allow_redirects=True) as resp:
        resp.raise_for_status()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def delete_artifact(owner: str, repo: str, token: str, artifact_id: int, dry_run: bool = False) -> bool:
    if dry_run:
        return True

    headers = github_headers(token)
    resp = requests.delete(
        f"{GITHUB_API}/repos/{owner}/{repo}/actions/artifacts/{artifact_id}",
        headers=headers,
        timeout=60,
    )
    if resp.status_code == 404:
        return True
    if resp.status_code not in (202, 204):
        resp.raise_for_status()
    return True


def load_manifest(path: Path) -> Dict[str, Any]:
    if path.exists():
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return {
        "version": 1,
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
        "entries": [],
    }


def save_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    manifest["updated_at"] = now_utc_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=False)


def cmd_pull(args: argparse.Namespace) -> int:
    token = args.token or os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Missing token. Set --token or GITHUB_TOKEN")

    owner, repo = parse_repo(args.repo)
    manifest_path = Path(args.manifest)
    dest_dir = Path(args.dest)

    artifacts = list_artifacts(owner, repo, token, prefix=args.prefix)
    if args.limit:
        artifacts = artifacts[: args.limit]

    if not artifacts:
        print("No matching artifacts found.")
        return 0

    manifest = load_manifest(manifest_path)
    print(f"Found {len(artifacts)} artifacts to pull.")

    downloaded = 0
    deleted = 0

    for item in artifacts:
        artifact_id = int(item["id"])
        name = item.get("name", "artifact")
        created_at = item.get("created_at", "")
        zip_name = f"{created_at[:10]}_{artifact_id}_{sanitize_filename(name)}.zip"
        out_path = dest_dir / zip_name

        print(f"Downloading #{artifact_id} {name} -> {out_path}")
        if args.dry_run:
            status = "dry-run"
        else:
            download_artifact_zip(owner, repo, token, artifact_id, out_path)
            status = "downloaded"
            downloaded += 1

        entry = {
            "artifact_id": artifact_id,
            "name": name,
            "created_at": created_at,
            "archive_download_url": item.get("archive_download_url"),
            "downloaded_at": now_utc_iso(),
            "local_zip": str(out_path),
            "deleted_remote": False,
            "status": status,
        }

        if args.delete_after_download:
            print(f"Deleting remote artifact #{artifact_id} {name}")
            ok = delete_artifact(owner, repo, token, artifact_id, dry_run=args.dry_run)
            entry["deleted_remote"] = bool(ok)
            if ok:
                deleted += 1

        manifest["entries"].append(entry)

    save_manifest(manifest_path, manifest)
    print(f"Done. Downloaded: {downloaded}. Deleted remote: {deleted}. Manifest: {manifest_path}")
    return 0


def cmd_cleanup_manifest(args: argparse.Namespace) -> int:
    token = args.token or os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Missing token. Set --token or GITHUB_TOKEN")

    owner, repo = parse_repo(args.repo)
    manifest_path = Path(args.manifest)

    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}")
        return 1

    manifest = load_manifest(manifest_path)
    entries = manifest.get("entries", [])

    to_delete = [
        entry for entry in entries
        if entry.get("status") in ("downloaded", "dry-run") and not entry.get("deleted_remote")
    ]

    if args.prefix:
        to_delete = [e for e in to_delete if str(e.get("name", "")).startswith(args.prefix)]

    if args.limit:
        to_delete = to_delete[: args.limit]

    if not to_delete:
        print("No downloaded artifacts pending deletion in manifest.")
        return 0

    print(f"Deleting {len(to_delete)} artifacts recorded in manifest.")
    deleted = 0

    by_id = {int(entry["artifact_id"]): entry for entry in entries if "artifact_id" in entry}
    for entry in to_delete:
        artifact_id = int(entry["artifact_id"])
        name = entry.get("name", "artifact")
        print(f"Deleting remote artifact #{artifact_id} {name}")
        ok = delete_artifact(owner, repo, token, artifact_id, dry_run=args.dry_run)
        if ok:
            by_id[artifact_id]["deleted_remote"] = True
            by_id[artifact_id]["deleted_at"] = now_utc_iso()
            deleted += 1

    save_manifest(manifest_path, manifest)
    print(f"Done. Deleted remote: {deleted}. Manifest updated: {manifest_path}")
    return 0


def cmd_cleanup_prefix(args: argparse.Namespace) -> int:
    token = args.token or os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Missing token. Set --token or GITHUB_TOKEN")

    owner, repo = parse_repo(args.repo)
    keep_names = set(args.keep_name or [])

    artifacts = list_artifacts(owner, repo, token, prefix=args.prefix)
    if not artifacts:
        print("No matching artifacts found.")
        return 0

    cutoff = None
    if args.older_than_days is not None:
        cutoff = datetime.now(timezone.utc).timestamp() - (args.older_than_days * 86400)

    candidates: List[Dict[str, Any]] = []
    for item in artifacts:
        name = item.get("name", "")
        if name in keep_names:
            continue

        if cutoff is not None:
            created_at = item.get("created_at")
            if not created_at:
                continue
            created_ts = parse_iso_utc(created_at).timestamp()
            if created_ts > cutoff:
                continue

        candidates.append(item)

    if args.limit:
        candidates = candidates[: args.limit]

    if not candidates:
        print("No artifacts matched cleanup conditions.")
        return 0

    print(f"Deleting {len(candidates)} artifacts (prefix={args.prefix!r}).")
    deleted = 0
    for item in candidates:
        artifact_id = int(item["id"])
        name = item.get("name", "artifact")
        created_at = item.get("created_at", "")
        print(f"Deleting #{artifact_id} {name} ({created_at})")
        ok = delete_artifact(owner, repo, token, artifact_id, dry_run=args.dry_run)
        if ok:
            deleted += 1

    print(f"Done. Deleted: {deleted}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download and clean up GitHub Actions artifacts safely",
    )
    parser.add_argument("--repo", required=True, help="GitHub repository in owner/repo format")
    parser.add_argument("--token", help="GitHub token (defaults to GITHUB_TOKEN env var)")

    subparsers = parser.add_subparsers(dest="command", required=True)

    pull = subparsers.add_parser("pull", help="Download artifacts and optionally delete immediately")
    pull.add_argument("--prefix", default="", help="Artifact name prefix filter (e.g. html-delta-)")
    pull.add_argument("--dest", default="artifacts/github", help="Destination directory for downloaded zip files")
    pull.add_argument("--manifest", default=DEFAULT_MANIFEST, help="Path to local manifest json")
    pull.add_argument("--limit", type=int, help="Maximum artifacts to process")
    pull.add_argument("--delete-after-download", action="store_true", help="Delete remote artifact after successful local download")
    pull.add_argument("--dry-run", action="store_true", help="Print actions without changing remote/local state")
    pull.set_defaults(func=cmd_pull)

    cleanup = subparsers.add_parser("cleanup-manifest", help="Delete artifacts that manifest marks as downloaded")
    cleanup.add_argument("--manifest", default=DEFAULT_MANIFEST, help="Path to local manifest json")
    cleanup.add_argument("--prefix", default="", help="Restrict deletion to artifact names with this prefix")
    cleanup.add_argument("--limit", type=int, help="Maximum artifacts to delete")
    cleanup.add_argument("--dry-run", action="store_true", help="Print actions without deleting")
    cleanup.set_defaults(func=cmd_cleanup_manifest)

    cleanup_prefix = subparsers.add_parser(
        "cleanup-prefix",
        help="Delete artifacts by name prefix (optionally older than N days)",
    )
    cleanup_prefix.add_argument("--prefix", required=True, help="Artifact name prefix filter")
    cleanup_prefix.add_argument(
        "--older-than-days",
        type=int,
        help="Delete only artifacts older than this many days",
    )
    cleanup_prefix.add_argument(
        "--keep-name",
        action="append",
        default=[],
        help="Artifact name to protect from deletion (repeatable)",
    )
    cleanup_prefix.add_argument("--limit", type=int, help="Maximum artifacts to delete")
    cleanup_prefix.add_argument("--dry-run", action="store_true", help="Print actions without deleting")
    cleanup_prefix.set_defaults(func=cmd_cleanup_prefix)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
