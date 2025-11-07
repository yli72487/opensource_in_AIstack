import os
import json
import logging
from datetime import datetime

from .src.github_scrape import scrape_repo
from .src.dummy_source import write_dummy

# Directory of this file (used to compute project root)
THIS_DIR = os.path.dirname(__file__)

def make_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def main():
    # Project root relative to this file
    root = os.path.abspath(os.path.join(THIS_DIR, os.pardir))
    data_clean_root = os.path.join(root, "data", "clean")

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_dir = os.path.join(data_clean_root, ts)
    os.makedirs(out_dir, exist_ok=True)

    log_path = os.path.join(out_dir, "pipeline_run.log")
    logger = make_logger(log_path)
    logger.info(f"Starting pipeline. Output dir: {out_dir}")

    # 1) Dummy step to show plumbing works
    dummy_dir = os.path.join(out_dir, "dummy")
    write_dummy(dummy_dir)
    logger.info("Dummy data written.")

    # 2) Scrape SGLang and vLLM
    targets = [
        ("sgl-project", "sglang"),
        ("vllm-project", "vllm"),
    ]
    # Additional: discover repos by topic and scrape their PRs/contributors
    topics = [
        "llm-inference",
        "open-source-llm",
        "llm-serving",
        "model-serving",
        "model-inference",
        "ai-inference",
        "model-deployment",
        "mlops",
        "inference-engine",
        "llm-ops",
        "open-source-ai",
    ]
    from .src.topic_scrape import find_repos_by_topics, scrape_repos_prs, scrape_users_from_prs, aggregate_contributors_master
    manifest = {"run_timestamp": ts, "outputs": {}, "notes": "Add GITHUB_TOKEN for better rate limits."}
    for owner, repo in targets:
        repo_dir = os.path.join(out_dir, repo.lower())
        os.makedirs(repo_dir, exist_ok=True)
        try:
            meta = scrape_repo(owner, repo, repo_dir, logger=logger)
            manifest["outputs"][f"{owner}/{repo}"] = meta
        except Exception as e:
            logger.exception(f"Failed on {owner}/{repo}: {e}")
            manifest["outputs"][f"{owner}/{repo}"] = {"error": str(e)}

    # Discover repos by topic and save findings
    topics_out = os.path.join(out_dir, "by_topics")
    try:
        repos = find_repos_by_topics(topics, topics_out, max_pages_per_topic=2, logger=logger)
        manifest["outputs"]["discovered_repos"] = {"count": len(repos), "path": topics_out}
        # Scrape PRs for discovered repos (this will create per-repo folders under topics_out)
        prs_meta = scrape_repos_prs(repos, os.path.join(topics_out, "prs"), logger=logger)
        manifest["outputs"]["discovered_repos_prs"] = {"count": len(prs_meta), "path": os.path.join(topics_out, "prs")}
        # Collect user profiles for PR authors and write per-repo users CSVs/JSONs
        try:
            users_index = scrape_users_from_prs(os.path.join(topics_out, "prs"), topics_out, logger=logger)
            manifest["outputs"]["pr_users_index"] = {"count": len(users_index), "path": os.path.join(topics_out, "pr_users_index.json")}
        except Exception as e:
            logger.exception(f"Failed to scrape users from PRs: {e}")
            manifest["outputs"]["pr_users_index"] = {"error": str(e)}

        # Aggregate per-repo contributor CSVs into a single master CSV
        try:
            master_csv = aggregate_contributors_master(os.path.join(topics_out, "prs"), topics_out, logger=logger)
            manifest["outputs"]["all_contributors_master_csv"] = {"path": master_csv}
        except Exception as e:
            logger.exception(f"Failed to aggregate contributors into master CSV: {e}")
            manifest["outputs"]["all_contributors_master_csv"] = {"error": str(e)}
    except Exception as e:
        logger.exception(f"Failed topic discovery/scrape: {e}")
        manifest["outputs"]["discovered_repos"] = {"error": str(e)}

    # 3) Save manifest
    with open(os.path.join(out_dir, "MANIFEST.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    logger.info("Pipeline finished.")

if __name__ == "__main__":
    main()
