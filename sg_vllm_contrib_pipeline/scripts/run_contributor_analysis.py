#!/usr/bin/env python3
"""
Run contributor analysis (headless) — reproduces the notebook plots and saves PNGs.
"""
import os
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

sns.set_theme(style='whitegrid')


def find_data_clean_dir():
    env = os.environ.get('DATA_CLEAN_DIR')
    if env:
        p = Path(env)
        if p.exists():
            return p.resolve()
    cwd = Path.cwd()
    for d in [cwd] + list(cwd.parents):
        candidate = d / 'data' / 'clean'
        if candidate.exists():
            return candidate.resolve()
    for rel in [Path('data/clean'), Path('../data/clean'), Path('../../data/clean')]:
        if rel.exists():
            return rel.resolve()
    return None


def normalize_org(s):
    s = str(s).strip()
    if not s:
        return ''
    s = s.replace('\n',' ').strip()
    for suffix in [', inc', ' inc.', ' inc', ' llc', ' llc.', ' ltd', ' ltd.', ' gmbh']:
        s = s.replace(suffix, '')
    return s


def to_topics(x):
    if pd.isna(x):
        return []
    s = str(x)
    for ch in '[]"\'':
        s = s.replace(ch, '')
    parts = [p.strip().lower() for p in s.split(',') if p.strip()]
    return parts


def main():
    base = find_data_clean_dir()
    if base is None:
        raise SystemExit("No data/clean found. Set DATA_CLEAN_DIR or run from repo root.")
    runs = sorted([p for p in base.iterdir() if p.is_dir()])
    if not runs:
        raise SystemExit(f'No run folders found under {base}')
    latest = runs[-1]
    by_topics = latest / 'by_topics'
    cand1 = by_topics / 'all_contributors_master_dedup.csv'
    cand2 = by_topics / 'all_contributors_master.csv'
    if cand1.exists():
        master_csv = cand1
    elif cand2.exists():
        master_csv = cand2
    else:
        raise SystemExit(f'No master CSV found in {by_topics}')
    print('Using master CSV:', master_csv)
    df = pd.read_csv(master_csv)
    print('Loaded rows,cols:', df.shape)

    # normalize
    df.columns = [c.strip() for c in df.columns]
    lc = {c.lower(): c for c in df.columns}
    login_col = lc.get('login', lc.get('username', df.columns[0]))
    company_col = lc.get('company')
    blog_col = lc.get('blog')
    topics_col = lc.get('matched_topics', lc.get('topics'))
    repo_col = lc.get('full_name', lc.get('repo_name', lc.get('repo')))
    created_col = lc.get('created_at')

    if company_col:
        df['company_clean'] = df[company_col].fillna('').astype(str).str.strip()
    else:
        df['company_clean'] = ''
    if blog_col:
        df['blog_clean'] = df[blog_col].fillna('').astype(str).str.strip()
    else:
        df['blog_clean'] = ''

    if topics_col:
        df['topics_list'] = df[topics_col].apply(to_topics)
    else:
        df['topics_list'] = [[] for _ in range(len(df))]

    # ensure a consistent contributor key
    df['login_key'] = df[login_col].astype(str)

    df_topics = df.copy()
    df_topics = df_topics.explode('topics_list')
    df_topics['topic'] = df_topics['topics_list'].fillna('').astype(str)

    if created_col and created_col in df.columns:
        # parse timestamps as UTC to avoid tz-aware vs tz-naive arithmetic errors
        df['created_at_dt'] = pd.to_datetime(df[created_col], errors='coerce', utc=True)
        now_utc = pd.Timestamp.now(tz='UTC')
        df['account_age_years'] = (now_utc - df['created_at_dt']).dt.days / 365.25
    else:
        df['created_at_dt'] = pd.NaT
        df['account_age_years'] = np.nan

    df['login_key'] = df[login_col].astype(str)

    fig_dir = by_topics / 'figures'
    fig_dir.mkdir(parents=True, exist_ok=True)

    # 4) contributors per topic
    topic_counts = df_topics[df_topics['topic'] != ''].groupby('topic')['login_key'].nunique().reset_index()
    topic_counts = topic_counts.rename(columns={'login_key':'unique_contributors'})
    topic_counts = topic_counts.sort_values('unique_contributors', ascending=False)
    top_n = min(30, len(topic_counts))
    plt.figure(figsize=(10, max(6, top_n*0.35)))
    sns.barplot(data=topic_counts.head(top_n), y='topic', x='unique_contributors', palette='viridis')
    plt.title(f'Top {top_n} topics by unique contributors')
    plt.xlabel('Unique contributors')
    plt.ylabel('Topic')
    plt.tight_layout()
    out_path = fig_dir / 'contributors_per_topic.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print('Saved:', out_path)

    # 5) top repos
    repo_col_use = repo_col if repo_col in df.columns else df.columns[0]
    repo_counts = df.groupby(repo_col_use)['login_key'].nunique().reset_index().rename(columns={'login_key':'unique_contributors'})
    repo_counts = repo_counts.sort_values('unique_contributors', ascending=False)
    top100 = repo_counts.head(100).copy()
    top30 = top100.head(30)
    plt.figure(figsize=(10, max(8, len(top30)*0.35)))
    sns.barplot(data=top30, y=repo_col_use, x='unique_contributors', palette='rocket')
    plt.title('Top 30 repositories by unique contributors')
    plt.xlabel('Unique contributors')
    plt.ylabel('Repository')
    plt.tight_layout()
    out_path = fig_dir / 'top30_repos_contributors.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print('Saved:', out_path)

    plt.figure(figsize=(12, 30))
    sns.barplot(data=top100, y=repo_col_use, x='unique_contributors', palette='magma')
    plt.title('Top 100 repositories by unique contributors')
    plt.xlabel('Unique contributors')
    plt.ylabel('Repository')
    plt.tight_layout()
    out_path = fig_dir / 'top100_repos_contributors.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print('Saved:', out_path)

    # 6) top 50 orgs
    df['company_norm'] = df['company_clean'].apply(normalize_org)
    company_counts = df[df['company_norm'] != ''].groupby('company_norm')['login_key'].nunique().reset_index().rename(columns={'login_key':'unique_contributors'})
    company_counts = company_counts.sort_values('unique_contributors', ascending=False)
    top50_companies = company_counts.head(50)
    plt.figure(figsize=(10, max(8, len(top50_companies)*0.25)))
    sns.barplot(data=top50_companies, y='company_norm', x='unique_contributors', palette='cubehelix')
    plt.title('Top 50 companies by unique contributors')
    plt.xlabel('Unique contributors')
    plt.ylabel('Company')
    plt.tight_layout()
    out_path = fig_dir / 'top50_companies.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print('Saved:', out_path)

    # 7) top company per topic
    topic_company = df_topics[df_topics['company_clean'] != ''].copy()
    topic_company['company_norm'] = topic_company['company_clean'].apply(normalize_org)
    grp = topic_company.groupby(['topic','company_norm'])['login_key'].nunique().reset_index().rename(columns={'login_key':'unique_contributors'})
    topic_top = grp.sort_values(['topic','unique_contributors'], ascending=[True, False]).groupby('topic').first().reset_index()
    topic_top = topic_top.sort_values('unique_contributors', ascending=False)
    top_n_topics = min(25, len(topic_top))
    plot_df = topic_top.head(top_n_topics).iloc[::-1]
    plt.figure(figsize=(10, max(6, top_n_topics*0.35)))
    sns.barplot(data=plot_df, x='unique_contributors', y='topic', color='tab:blue')
    for i, (_, row) in enumerate(plot_df.iterrows()):
        plt.text(row['unique_contributors'] + max(plot_df['unique_contributors'])*0.01, i, str(row['company_norm']), va='center')
    plt.title('Top company per topic (contributors)')
    plt.xlabel('Unique contributors (top company)')
    plt.ylabel('Topic')
    plt.tight_layout()
    out_path = fig_dir / 'topic_top_companies.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print('Saved:', out_path)

    topic_top.to_csv(by_topics / 'topic_top_companies.csv', index=False)

    # 8) simple stack mapping
    stack_map = {
        'data': ['data', 'datasets', 'data-processing', 'data-engineering'],
        'model': ['model', 'models', 'training', 'nlp', 'vision', 'lm', 'llm', 'transformer'],
        'infrastructure': ['infrastructure', 'infra', 'deployment', 'serving', 'docker', 'k8s', 'gpu'],
        'tooling': ['tools', 'sdk', 'cli', 'notebook', 'editor', 'monitoring'],
        'evaluation': ['evaluation', 'benchmarks', 'metrics', 'testing']
    }
    def map_topic_to_stack(topic):
        t = str(topic).lower()
        for layer, keywords in stack_map.items():
            for k in keywords:
                if k in t:
                    return layer
        return 'other'
    df_topics['stack'] = df_topics['topic'].apply(map_topic_to_stack)
    stack_pivot = df_topics[df_topics['topic'] != ''].groupby('stack')['login_key'].nunique().reset_index().rename(columns={'login_key':'unique_contributors'})
    stack_pivot = stack_pivot.sort_values('unique_contributors', ascending=False)
    plt.figure(figsize=(8,4))
    sns.barplot(data=stack_pivot, x='stack', y='unique_contributors', palette='pastel')
    plt.title('Contributors by AI stack layer (simple mapping)')
    plt.xlabel('Stack layer')
    plt.ylabel('Unique contributors')
    plt.tight_layout()
    out_path = fig_dir / 'contributors_by_stack.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print('Saved:', out_path)

    # 9) heatmap
    top_orgs = company_counts.head(30)['company_norm'].tolist()
    heat_df = df_topics[df_topics['company_clean'] != '']
    heat_df['company_norm'] = heat_df['company_clean'].apply(normalize_org)
    heat_df = heat_df[heat_df['company_norm'].isin(top_orgs) & (heat_df['topic'] != '')]
    pivot = heat_df.groupby(['topic','company_norm'])['login_key'].nunique().unstack(fill_value=0)
    top_topics = topic_counts.head(20)['topic'].tolist()
    pivot = pivot.reindex(top_topics).fillna(0)
    plt.figure(figsize=(14,8))
    sns.heatmap(np.log1p(pivot), cmap='YlGnBu', linewidths=.5, annot=False)
    plt.title('Log(1+contributors) by topic (rows) and organization (cols)')
    plt.tight_layout()
    out_path = fig_dir / 'topic_org_heatmap.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print('Saved:', out_path)

    # 10) temporal
    if 'created_at_dt' in df.columns and df['created_at_dt'].notna().any():
        df['created_year'] = df['created_at_dt'].dt.year
        year_counts = df.groupby('created_year')['login_key'].nunique().reset_index().dropna()
        plt.figure(figsize=(10,4))
        sns.lineplot(data=year_counts, x='created_year', y='login_key', marker='o')
        plt.title('Number of unique contributors by account creation year')
        plt.xlabel('Year')
        plt.ylabel('Unique contributors')
        plt.tight_layout()
        out_path = fig_dir / 'contributors_by_creation_year.png'
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close()
        print('Saved:', out_path)

    # 11) followers vs repos
    if 'followers' in df.columns and 'public_repos' in df.columns:
        sample = df.dropna(subset=['followers','public_repos']).sample(n=min(10000, len(df)), random_state=42)
        plt.figure(figsize=(8,6))
        sns.scatterplot(data=sample, x='public_repos', y='followers', alpha=0.6)
        plt.xscale('symlog')
        plt.yscale('symlog')
        plt.xlabel('public_repos (symlog)')
        plt.ylabel('followers (symlog)')
        plt.title('Followers vs public_repos (sample)')
        out_path = fig_dir / 'followers_vs_repos.png'
        plt.tight_layout()
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close()
        print('Saved:', out_path)

    # save cleaned csv
    clean_out = by_topics / 'all_contributors_master_dedup_cleaned.csv'
    df.to_csv(clean_out, index=False)
    print('Saved cleaned CSV:', clean_out)

    print('Done')


if __name__ == '__main__':
    main()
