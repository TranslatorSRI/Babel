#!/usr/bin/env python3
#
# contributors.py
#
# This script is intended to be run as part of a GitHub Action with a GITHUB_TOKEN that allows access to the GitHub API.
# It uses this to generate a list of all issue- and PR-related contributors to this project.
# Unlike https://github.com/github/contributors, this doesn't include contributors in the GitHub sense (those who have
# committed code to default branches), and unlike https://github.com/github/issue-metrics, it isn't intended to produce
# statistics on issues and PRs, but to produce a simple, data-rich list of users that we could store in the repository
# or turn into Markdown.
#
# Thanks to ChatGPT for producing an initial version of this script.
#

import os
import csv
from collections import defaultdict

import requests
import logging

logging.basicConfig(level=logging.DEBUG)

# Load environmental variables.
token = os.environ["GITHUB_TOKEN"]
repo = os.environ["GITHUB_REPOSITORY"]  # e.g. "owner/repo"

# Step 1. Set up dictionaries to track contributions.
contributions_by_username = {}
overall_contribution_counts = defaultdict(set)
def add_user_contribution(user_obj, contribution_type, contribution):
    if user_obj is None or contribution is None:
        return

    login = user_obj["login"]
    login_url = user_obj["html_url"]
    avatar_url = user_obj["avatar_url"]

    if login not in contributions_by_username:
        contributions_by_username[login] = {
            "login": login,
            "login_url": login_url,
            "avatar_url": avatar_url,
            "contributions": set(),
            "contribution_by_type": dict(),
        }

    if contribution_type not in contributions_by_username[login]["contribution_by_type"]:
        contributions_by_username[login]["contribution_by_type"][contribution_type] = set()

    contributions_by_username[login]["contributions"].add(contribution["html_url"])
    contributions_by_username[login]["contribution_by_type"][contribution_type].add(contribution["html_url"])
    overall_contribution_counts[contribution_type].add(contribution["html_url"])

# Step 2. Collect information about all issues.
repo_issues_url = f"https://api.github.com/repos/{repo}/issues"
headers = {
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28",
    # We only want to `body_text` for issues -- we probably won't use them.
    # See https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues for options.
    "Accept": "application/vnd.github.text+json",
}
params = {
    "state": "all",
    "per_page": 100,
}

while repo_issues_url:
    logging.debug(f"Fetching issues from {repo_issues_url}")
    response = requests.get(repo_issues_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    for issue in data:
        # Is this a pull request?
        contribution_type = "issue"
        if "pull_request" in issue and "html_url" in issue["pull_request"]:
            contribution_type = "pull_request"

        # Add contributions.
        add_user_contribution(
            user_obj=issue["user"],
            contribution_type=f"{contribution_type}_created",
            contribution=issue,
        )
        add_user_contribution(
            user_obj=issue["closed_by"],
            contribution_type=f"{contribution_type}_closed",
            contribution=issue,
        )
        for assignee in issue["assignees"]:
            add_user_contribution(
                user_obj=assignee,
                contribution_type=f"{contribution_type}_assigned",
                contribution=issue,
            )

        # TODO: count comments.

    # handle pagination
    repo_issues_url = response.links.get("next", {}).get("url")
    params = None  # only include params on the first request

overall_contribution_count_summary = ", ".join(f"{contribution_type}: {len(overall_contribution_counts[contribution_type])}" for contribution_type in overall_contribution_counts)
logging.info(f"Found {len(contributions_by_username)} contributors: {overall_contribution_count_summary}")

# Step 3. Summarize results.
contributor_rows = 0
with open("./tmp/contributors.tsv", "w") as f:
    fieldnames = [
        "login",
        "login_url",
        "avatar_url"
    ]
    fieldnames.extend(sorted(overall_contribution_counts.keys()))
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    sorted_contributors = sorted(contributions_by_username.values(), key=lambda x: len(x["contributions"]), reverse=True)
    for contributor in sorted_contributors:
        row = {
            'login': contributor['login'],
            'login_url': contributor['login_url'],
            'avatar_url': contributor['avatar_url'],
        }

        for contribution_type in contributor['contribution_by_type']:
            row[contribution_type] = len(contributor['contribution_by_type'][contribution_type])

        writer.writerow(row)
        contributor_rows += 1

logging.info(f"Wrote {contributor_rows} contributors to ./tmp/contributors.tsv")
