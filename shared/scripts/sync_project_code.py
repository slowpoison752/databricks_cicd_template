"""
Sync project code from GitHub to Databricks workspace
Can be run as a Databricks job task
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, cwd=None):
    """Execute shell command and return output"""
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=cwd,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        sys.exit(1)
    return result.stdout


def sync_repository(repo_url, repo_ref, target_path):
    """Clone or update repository"""

    print(f"Repository: {repo_url}")
    print(f"Reference: {repo_ref}")
    print(f"Target: {target_path}")

    # Create parent directory if needed
    parent_dir = str(Path(target_path).parent)
    os.makedirs(parent_dir, exist_ok=True)

    if os.path.exists(target_path):
        print(f"Updating existing repository at {target_path}")

        # Fetch latest changes
        run_command("git fetch origin", cwd=target_path)

        # Checkout specified ref
        run_command(f"git checkout {repo_ref}", cwd=target_path)

        # Pull latest changes
        run_command("git pull", cwd=target_path)

        # Get current commit
        commit = run_command("git rev-parse HEAD", cwd=target_path).strip()
        print(f"Updated to commit: {commit}")

    else:
        print(f"Cloning repository to {target_path}")

        # Clone repository
        run_command(f"git clone -b {repo_ref} {repo_url} {target_path}")

        # Get current commit
        commit = run_command("git rev-parse HEAD", cwd=target_path).strip()
        print(f"Cloned at commit: {commit}")

    # Show latest commit info
    commit_info = run_command("git log -1 --oneline", cwd=target_path).strip()
    print(f"Latest commit: {commit_info}")

    return commit


def main():
    parser = argparse.ArgumentParser(
        description="Sync project code from GitHub to workspace"
    )
    parser.add_argument("--repo-url", required=True, help="Repository URL")
    parser.add_argument("--repo-ref", default="main", help="Branch/tag/commit")
    parser.add_argument("--target-path", required=True, help="Target workspace path")

    args = parser.parse_args()

    try:
        commit = sync_repository(args.repo_url, args.repo_ref, args.target_path)
        print(f"\n✅ Project code synced successfully!")
        print(f"Commit: {commit}")

    except Exception as e:
        print(f"\n❌ Sync failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()