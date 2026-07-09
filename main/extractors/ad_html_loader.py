import gzip
import os
import re
import tempfile
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from enum import Enum


# class AdType(Enum):
#     PROPERTY = 1
#     JOB = 2


def _atomic_write(path, data, *, binary=False):
    """Write ``data`` to ``path`` atomically.

    Writes to a temp file in the same directory then ``os.replace``s it into
    place, so a failed/partial write can never truncate an existing good file.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    mode = "wb" if binary else "w"
    open_kwargs = {} if binary else {"encoding": "utf-8"}
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, mode, **open_kwargs) as handle:
            handle.write(data)
        os.replace(tmp, path)
    except BaseException:
        # Leave any existing file untouched; discard the temp.
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def save_ad_html(project_name, uid, html_str, snapshot_dir=None, today=None):
    """Persist ad HTML for ``uid``.

    The canonical copy ``{project}/html_extracted/{uid}.html`` is written
    atomically. When the content differs from the previous canonical (or none
    existed yet), a gzipped, date-stamped snapshot is also archived under
    ``{project}/html_snapshots/{uid}.{YYYYMMDD}.html.gz`` so prior versions are
    never overwritten. Unchanged re-downloads produce no snapshot.

    Returns the canonical file path.
    """
    canonical_path = os.path.join(project_name, "html_extracted", f"{uid}.html")

    previous = None
    if os.path.exists(canonical_path):
        with open(canonical_path, encoding="utf-8") as handle:
            previous = handle.read()
    changed = previous is None or previous != html_str

    _atomic_write(canonical_path, html_str)

    if changed:
        if snapshot_dir is None:
            snapshot_dir = os.path.join(project_name, "html_snapshots")
        day = today or datetime.now().strftime("%Y%m%d")
        snapshot_path = os.path.join(snapshot_dir, f"{uid}.{day}.html.gz")
        _atomic_write(snapshot_path, gzip.compress(html_str.encode("utf-8")), binary=True)

    return canonical_path


def download_and_save_ad_html(url, projectName, finnkode):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    save_ad_html(projectName, finnkode, str(soup))
    return soup


def load_or_fetch_ad_html(url, projectName, auto_save_new=True, force_save=False, isNAV=False):
    """
    Fetches ad data from the given URL and saves the HTML content if specified.
    :param url: The URL of the ad to extract data from.
    :param projectName: The name of the project folder to save the HTML content.
    :param force_save: If True, forces re-fetching of the ad data.
    :return: A dictionary containing extracted ad data.
    """
    if isNAV:
        match = re.search(r'stilling/([\w-]+)$', url)
    else:
        match = re.search(r'(\d+)(?!.*\d)', url)

    if not match:
        raise ValueError(f"Could not extract UID from URL: {url}")
    uid = match.group(1)
    html_file_path = f'{projectName}/html_extracted/{uid}.html'
    exists = os.path.exists(html_file_path)

    if (force_save):
        time.sleep(0.1)
        print(f"Force-saving HTML content for {uid}.")
        return download_and_save_ad_html(url, projectName, uid)

    elif (exists or not auto_save_new):
        with open(html_file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file.read(), 'html.parser')
            return soup
    else:
        time.sleep(0.1)
        print(f"Saving HTML content for {uid}.")
        return download_and_save_ad_html(url, projectName, uid)
