import os
import time
import logging
from typing import List, Dict, Optional, Set, Tuple

from .utils import make_session, save_json, get_json_with_retry, request_with_rate_limit
from .github_scrape import users_details, to_dataframe_users
from typing import Iterable
import json
import pandas as pd

GITHUB_API = "https://api.github.com"


def search_repos_for_topic(topic: str, session=None, max_pages: int = 5, logger: Optional[logging.Logger] = None) -> List[Dict]:
    """Search repositories for a given topic using the search API.
    Returns a list of repo dicts (as returned by the search endpoint).
    """
    if session is None:
        session = make_session()
    all_items = []
    for page in range(1, max_pages + 1):
        url = f"{GITHUB_API}/search/repositories"
        params = {"q": f"topic:{topic}", "per_page": 100, "page": page}
        r = request_with_rate_limit(session, 'GET', url, params=params, timeout=60, logger=logger)
        if r.status_code != 200:
            if logger:
                logger.warning(f"Search repos for topic {topic} page {page} failed: {r.status_code} {getattr(r, 'text', '')[:200]}")
            break
        data = r.json()
        items = data.get("items", [])
        if not items:
            break
        all_items.extend(items)
        if logger: logger.info(f"Found {len(items)} repos for topic {topic} on page {page}")
        # Respectful pause
        time.sleep(0.2)
    return all_items


def get_repo_topics(owner: str, repo: str, session=None, logger: Optional[logging.Logger]=None) -> List[str]:
    if session is None:
        session = make_session()
    url = f"{GITHUB_API}/repos/{owner}/{repo}/topics"
    # Topics endpoint requires a preview accept header; override for this request
    headers = {"Accept": "application/vnd.github.mercy-preview+json"}
    r = request_with_rate_limit(session, 'GET', url, headers=headers, timeout=30, logger=logger)
    if r.status_code != 200:
        if logger: logger.warning(f"Failed to fetch topics for {owner}/{repo}: {r.status_code}")
        return []
    data = r.json()
    return data.get("names", [])


def list_pull_requests(owner: str, repo: str, session=None, logger: Optional[logging.Logger]=None) -> List[Dict]:
    if session is None:
        session = make_session()
    out = []
    page = 1
    while True:
        url = f"{GITHUB_API}/repos/{owner}/{repo}/pulls"
        params = {"state": "all", "per_page": 100, "page": page}
        r = request_with_rate_limit(session, 'GET', url, params=params, timeout=60, logger=logger)
        if r.status_code != 200:
            if logger: logger.warning(f"Failed to list PRs for {owner}/{repo} p{page}: {r.status_code}")
            break
        rows = r.json()
        if not rows:
            break
        out.extend(rows)
        page += 1
        time.sleep(0.2)
    return out


def is_human_user(user_obj: Dict) -> bool:
    """Return True if the actor appears to be a human (not a bot)."""
    if not user_obj:
        return False
    # GitHub returns type 'User' for real users and 'Bot' for bots
    if user_obj.get("type") != "User":
        return False
    login = user_obj.get("login", "")
    # Exclude typical bot suffixes or usernames that clearly contain 'bot'
    if login.lower().endswith("[bot]") or login.lower().endswith("bot"):
        return False
    return True


def find_repos_by_topics(topics: List[str], out_dir: str, max_pages_per_topic: int = 3, logger: Optional[logging.Logger]=None) -> Dict[str, Dict]:
    """Search GitHub for repositories matching any of the provided topics.
    Returns a dict keyed by full_name (owner/repo) with metadata including topics found.
    Also writes a `repos_by_topic.json` file in out_dir.
    """
    os.makedirs(out_dir, exist_ok=True)
    session = make_session()
    repos: Dict[str, Dict] = {}
    for topic in topics:
        if logger: logger.info(f"Searching topic: {topic}")
        items = search_repos_for_topic(topic, session=session, max_pages=max_pages_per_topic, logger=logger)
        for item in items:
            full = item.get("full_name")
            if not full:
                continue
            if full not in repos:
                repos[full] = {
                    "full_name": full,
                    "owner": item.get("owner", {}).get("login"),
                    "name": item.get("name"),
                    "html_url": item.get("html_url"),
                    "description": item.get("description"),
                    "stargazers_count": item.get("stargazers_count", 0),
                    "forks_count": item.get("forks_count", 0),
                    "topics": [],
                    "matched_topics": set()
                }
            repos[full]["matched_topics"].add(topic)
    # Fetch full topics list per repo (quiet pause between requests)
    for full, info in repos.items():
        owner = info["owner"]
        repo = info["name"]
        t = get_repo_topics(owner, repo, session=session, logger=logger)
        info["topics"] = t
        # normalize matched_topics to list
        info["matched_topics"] = sorted(list(info["matched_topics"]))
        time.sleep(0.1)

    out_path = os.path.join(out_dir, "repos_by_topic.json")
    # Convert any non-serializable sets
    serializable = {k: {**v, "matched_topics": v.get("matched_topics", [])} for k, v in repos.items()}
    save_json(serializable, out_path)
    if logger: logger.info(f"Saved {len(repos)} unique repos to {out_path}")
    return serializable


def scrape_repos_prs(repos: Dict[str, Dict], out_dir: str, logger: Optional[logging.Logger]=None) -> Dict[str, Dict]:
    """For each repo in repos (dict keyed by full_name), fetch PRs and save per-repo outputs.
    Returns a metadata dict summarizing results.
    """
    session = make_session()
    results = {}
    os.makedirs(out_dir, exist_ok=True)
    for full, info in repos.items():
        owner, name = full.split("/")
        repo_dir = os.path.join(out_dir, name)
        os.makedirs(repo_dir, exist_ok=True)
        if logger: logger.info(f"Scraping PRs for {full}")
        prs = list_pull_requests(owner, name, session=session, logger=logger)
        # Filter human contributors from PR authors
        pr_records = []
        human_logins = set()
        for pr in prs:
            author = pr.get("user") or {}
            is_human = is_human_user(author)
            record = {
                "number": pr.get("number"),
                "title": pr.get("title"),
                "user": author.get("login"),
                "user_type": author.get("type"),
                "is_human": is_human,
                "created_at": pr.get("created_at"),
                "closed_at": pr.get("closed_at"),
                "merged_at": pr.get("merged_at"),
                "state": pr.get("state"),
                "html_url": pr.get("html_url"),
            }
            pr_records.append(record)
            if is_human:
                human_logins.add(author.get("login"))
        # Save PRs
        prs_path = os.path.join(repo_dir, "pull_requests.json")
        save_json(pr_records, prs_path)

        # Save a summary
        meta = {
            "full_name": full,
            "owner": owner,
            "name": name,
            "num_prs": len(pr_records),
            "human_contributors": sorted(list(human_logins)),
            "topics": info.get("topics", []),
            "matched_topics": info.get("matched_topics", []),
            "outputs": {
                "prs_json": prs_path
            }
        }
        results[full] = meta
        if logger: logger.info(f"Saved PRs for {full}: {meta['num_prs']} PRs, {len(human_logins)} human contributors")
        time.sleep(0.1)
    # Save aggregate results
    agg_path = os.path.join(out_dir, "repos_prs_summary.json")
    save_json(results, agg_path)
    if logger: logger.info(f"Wrote PR summaries to {agg_path}")
    return results


def scrape_users_from_prs(prs_parent_dir: str, by_topics_dir: str, logger: Optional[logging.Logger]=None) -> Dict[str, Dict]:
    """Read per-repo `pull_requests.json` files under `prs_parent_dir`, collect unique user logins
    (PR authors and merged_by when present), fetch their GitHub user details and save per-repo
    `users.json` and `contributors.csv` files. Also returns a map of repo -> users fetched.
    ``prs_parent_dir`` expected layout: <by_topics_dir>/prs/<repo_name>/pull_requests.json
    ``by_topics_dir`` is used to write `repos_by_topic.csv` (if `repos_by_topic.json` exists).
    """
    os.makedirs(prs_parent_dir, exist_ok=True)
    session = make_session()
    results = {}
    # iterate subdirectories in prs_parent_dir
    for entry in sorted(os.listdir(prs_parent_dir)):
        repo_dir = os.path.join(prs_parent_dir, entry)
        if not os.path.isdir(repo_dir):
            continue
        prs_path = os.path.join(repo_dir, "pull_requests.json")
        if not os.path.exists(prs_path):
            if logger: logger.info(f"No pull_requests.json for {entry}, skipping")
            continue
        if logger: logger.info(f"Collecting users from PRs in {entry}")
        try:
            with open(prs_path, "r", encoding="utf-8") as fh:
                pr_rows = json.load(fh)
        except Exception as e:
            if logger: logger.warning(f"Failed to read {prs_path}: {e}")
            continue

        # collect logins from PR 'user' (could be dict or string) and optionally 'merged_by'
        logins: Set[str] = set()
        for pr in pr_rows:
            user = pr.get("user")
            # support both shapes: {'login': 'x'} or a plain string 'x'
            if isinstance(user, dict):
                login = user.get("login")
                if login:
                    logins.add(login)
            elif isinstance(user, str):
                logins.add(user)
            # merged_by is less common in saved summaries, but handle both shapes too
            merged_by = pr.get("merged_by")
            if isinstance(merged_by, dict):
                mlogin = merged_by.get("login")
                if mlogin:
                    logins.add(mlogin)
            elif isinstance(merged_by, str):
                logins.add(merged_by)

        logins_list = sorted([l for l in logins if l])
        if not logins_list:
            if logger: logger.info(f"No user logins found for {entry}")
            results[entry] = {"users_fetched": 0, "users": []}
            continue

        # fetch user details using existing helper
        if logger: logger.info(f"Fetching {len(logins_list)} users for {entry}")
        user_rows = users_details(logins_list, logger=logger)

        # save JSON and CSV under the repo_dir
        users_json_path = os.path.join(repo_dir, "users.json")
        try:
            save_json(user_rows, users_json_path)
        except Exception:
            # fallback to plain json dump
            with open(users_json_path, "w", encoding="utf-8") as fh:
                json.dump(user_rows, fh, ensure_ascii=False, indent=2)

        df_users = to_dataframe_users(user_rows)
        users_csv_path = os.path.join(repo_dir, "contributors.csv")
        try:
            df_users.to_csv(users_csv_path, index=False)
        except Exception as e:
            if logger: logger.warning(f"Failed to write CSV for {entry}: {e}")

        results[entry] = {
            "users_fetched": int(len(df_users)),
            "users_json": users_json_path,
            "users_csv": users_csv_path,
            "logins_requested": logins_list
        }
        if logger: logger.info(f"Saved users for {entry}: {results[entry]['users_fetched']} rows")
        time.sleep(0.1)

    # Try to produce a repos_by_topic.csv if repos_by_topic.json exists nearby
    repos_by_topic_json = os.path.join(by_topics_dir, "repos_by_topic.json")
    csv_out = os.path.join(by_topics_dir, "repos_by_topic.csv")
    if os.path.exists(repos_by_topic_json):
        try:
            with open(repos_by_topic_json, "r", encoding="utf-8") as fh:
                repos_map = json.load(fh)
            rows = []
            for full, info in repos_map.items():
                rows.append({
                    "full_name": full,
                    "owner": info.get("owner"),
                    "name": info.get("name"),
                    "html_url": info.get("html_url"),
                    "stargazers_count": info.get("stargazers_count"),
                    "forks_count": info.get("forks_count"),
                    "topics": ",".join(info.get("topics", [])),
                    "matched_topics": ",".join(info.get("matched_topics", []))
                })
            df_repos = pd.DataFrame.from_records(rows)
            df_repos.to_csv(csv_out, index=False)
            if logger: logger.info(f"Wrote repos_by_topic CSV to {csv_out}")
        except Exception as e:
            if logger: logger.warning(f"Failed to write repos_by_topic.csv: {e}")

    # Save an index file of user outputs
    index_path = os.path.join(by_topics_dir, "pr_users_index.json")
    save_json(results, index_path)
    if logger: logger.info(f"Wrote PR users index to {index_path}")
    return results


def aggregate_contributors_master(prs_parent_dir: str, by_topics_dir: str, out_csv: Optional[str]=None, logger: Optional[logging.Logger]=None) -> str:
    """Aggregate per-repo contributor/user CSVs under `prs_parent_dir` into a single master CSV.

    - Looks for `contributors.csv` first, falls back to `users.csv` in each repo folder.
    - Augments rows with repo metadata from `{by_topics_dir}/repos_by_topic.json` when available.
    - Writes `all_contributors_master.csv` under `by_topics_dir` (or `out_csv` if provided) and returns the path.
    """
    import pandas as pd
    import json
    from pathlib import Path

    prs_parent = Path(prs_parent_dir)
    by_topics = Path(by_topics_dir)
    if out_csv:
        out_path = Path(out_csv)
    else:
        out_path = by_topics / 'all_contributors_master.csv'

    # Load repos map if present
    repos_map = {}
    repos_json = by_topics / 'repos_by_topic.json'
    if repos_json.exists():
        try:
            with open(repos_json, 'r', encoding='utf-8') as fh:
                repos_map = json.load(fh)
        except Exception:
            repos_map = {}

    rows = []
    repo_csv_count = 0
    if prs_parent.exists():
        for repo_dir in sorted([p for p in prs_parent.iterdir() if p.is_dir()]):
            csv_path = repo_dir / 'contributors.csv'
            if not csv_path.exists():
                csv_path = repo_dir / 'users.csv'
            if not csv_path.exists():
                continue
            try:
                df = pd.read_csv(csv_path)
            except Exception:
                if logger: logger.warning(f"Failed to read CSV {csv_path}, skipping")
                continue

            repo_name = repo_dir.name
            # find matching full_name in repos_map
            full_match = None
            for full, info in repos_map.items():
                if info.get('name') == repo_name:
                    full_match = (full, info)
                    break

            owner = full_match[1]['owner'] if full_match else None
            full_name = full_match[0] if full_match else None
            topics = ','.join(full_match[1].get('topics', [])) if full_match else ''
            matched = ','.join(full_match[1].get('matched_topics', [])) if full_match else ''

            df['repo_dir'] = str(repo_dir)
            df['repo_name'] = repo_name
            df['full_name'] = full_name
            df['owner'] = owner
            df['topics'] = topics
            df['matched_topics'] = matched

            rows.append(df)
            repo_csv_count += 1

    if not rows:
        raise RuntimeError(f"No contributor/user CSVs found under {prs_parent_dir}")

    all_df = pd.concat(rows, ignore_index=True, sort=False)
    # Ensure output dir exists
    out_path.parent.mkdir(parents=True, exist_ok=True)
    all_df.to_csv(out_path, index=False)
    if logger: logger.info(f"Wrote master CSV: {out_path} (rows={len(all_df)} from {repo_csv_count} repo CSVs)")
    return str(out_path)
