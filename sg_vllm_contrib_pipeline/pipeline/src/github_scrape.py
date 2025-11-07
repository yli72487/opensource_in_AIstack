import os
import logging
from typing import List, Dict, Optional, Tuple
import time

import pandas as pd
from tqdm import tqdm

from .utils import make_session, save_json, get_json_with_retry, request_with_rate_limit

GITHUB_API = "https://api.github.com"

def list_contributors(owner: str, repo: str, include_anon: bool=True, logger: Optional[logging.Logger]=None) -> List[Dict]:
    session = make_session()
    all_rows = []
    page = 1
    while True:
        url = f"{GITHUB_API}/repos/{owner}/{repo}/contributors"
        params = {"per_page": 100, "page": page}
        if include_anon:
            params["anon"] = "true"
        r = request_with_rate_limit(session, 'GET', url, params=params, timeout=60, logger=logger)
        if r.status_code != 200:
            if logger: logger.warning(f"Failed to list contributors p{page}: {r.status_code} {getattr(r, 'text', '')[:200]}")
            r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        all_rows.extend(rows)
        if logger: logger.info(f"Fetched {len(rows)} contributors on page {page}")
        page += 1
        time.sleep(0.2)
    return all_rows

def contributor_stats(owner: str, repo: str, logger: Optional[logging.Logger]=None) -> List[Dict]:
    session = make_session()
    url = f"{GITHUB_API}/repos/{owner}/{repo}/stats/contributors"
    data = get_json_with_retry(session, url, logger=logger)
    return data

def users_details(user_logins: List[str], logger: Optional[logging.Logger]=None) -> List[Dict]:
    session = make_session()
    out = []
    for login in tqdm(user_logins, desc="users", leave=False):
        if not login:
            continue
        url = f"{GITHUB_API}/users/{login}"
        r = request_with_rate_limit(session, 'GET', url, timeout=60, logger=logger)
        if r.status_code != 200:
            if logger: logger.info(f"Failed user {login}: {r.status_code}")
            continue
        try:
            out.append(r.json())
        except Exception:
            if logger: logger.warning(f"Failed to parse user JSON for {login}")
            continue
        time.sleep(0.1)
    return out

def to_dataframe_contributors(rows: List[Dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    # Flatten a few useful fields
    records = []
    for r in rows:
        record = {
            "login": r.get("login"),
            "type": r.get("type"),
            "contributions": r.get("contributions"),
            "id": (r.get("id") if isinstance(r.get("id"), int) else None),
            "html_url": r.get("html_url"),
        }
        # Anonymous contributors will not have login/id; their 'name' might appear in 'login' field when anon=true
        if record["login"] is None and isinstance(r, dict):
            record["login"] = r.get("name")  # fallback
        records.append(record)
    return pd.DataFrame.from_records(records)

def to_dataframe_users(rows: List[Dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    keep = ["login","name","company","blog","location","email","hireable","bio","twitter_username","public_repos","followers","following","created_at","updated_at"]
    return pd.DataFrame([{k: r.get(k) for k in keep} for r in rows])

def scrape_repo(owner: str, repo: str, out_dir: str, logger: Optional[logging.Logger]=None) -> Dict:
    os.makedirs(out_dir, exist_ok=True)
    if logger: logger.info(f"Scraping {owner}/{repo} -> {out_dir}")

    # Contributors (paginated list)
    contributors_rows = list_contributors(owner, repo, include_anon=True, logger=logger)
    df_contrib = to_dataframe_contributors(contributors_rows)
    contrib_path = os.path.join(out_dir, "contributors.csv")
    df_contrib.to_csv(contrib_path, index=False)

    # Contributor stats (weekly additions/deletions/commits)
    stats_rows = contributor_stats(owner, repo, logger=logger)
    stats_path = os.path.join(out_dir, "contributor_stats.json")
    save_json(stats_rows, stats_path)

    # User details (for logins we have)
    logins = [x for x in df_contrib["login"].dropna().unique().tolist() if isinstance(x, str)]
    user_rows = users_details(logins, logger=logger)
    df_users = to_dataframe_users(user_rows)
    users_path = os.path.join(out_dir, "users.csv")
    df_users.to_csv(users_path, index=False)

    meta = {
        "owner": owner,
        "repo": repo,
        "outputs": {
            "contributors_csv": contrib_path,
            "contributor_stats_json": stats_path,
            "users_csv": users_path
        },
        "counts": {
            "contributors": int(len(df_contrib)),
            "users": int(len(df_users)),
            "stats_entries": int(len(stats_rows) if isinstance(stats_rows, list) else 0)
        }
    }
    if logger: logger.info(f"Done {owner}/{repo}: {meta['counts']}")
    return meta
