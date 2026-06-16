#!/usr/bin/env python
import os
import re

import git

dir_path=os.path.dirname(os.path.realpath(__file__))
CHANGELOG=f"{dir_path}/../CHANGELOG.md"

def get_latest_tag() -> str:
    repo = git.Repo(f"{dir_path}/..")
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
    return tags[-1].__str__().strip('v')

def verify_changelog():
    md_headings = re.compile("^#{2} .*")
    date_vals = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    release_headings = []
    with open(CHANGELOG, "r") as f:
        lines = f.readlines()
        for line in lines:
            if md_headings.match(line):
                release_headings.append(line)

    cl_release_heading = release_headings[0].split()
    cl_release = cl_release_heading[1]
    cl_date = cl_release_heading[2].strip('[').strip(']')

    tag = get_latest_tag()
    if cl_release != tag:
        raise Exception(f"Tag in CHANGELOG.md ({cl_release}) does not match latest git tag ({tag})")

    if not date_vals.match(cl_date):
        raise Exception(f"Date ({cl_date}) for this release, does not conform to YYYY-MM-DD pattern.")

    print(f"Release {cl_release} on {cl_date} in CHANGELOG.md OK ✓.")


def main():
    print("on-release start")
    print("TODO - under construction")
    verify_changelog()

if __name__ == "__main__":
    main()