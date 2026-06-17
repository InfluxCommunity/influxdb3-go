#!/usr/bin/env python
import os
import re

import git

dir_path=os.path.dirname(os.path.realpath(__file__))
CHANGELOG=f"{dir_path}/../CHANGELOG.md"
VERSION_FILE=f"{dir_path}/../influxdb3/version.go"

TAG_MAJ = 0
TAG_MIN = 1
TAG_INC = 2

def get_latest_tag() -> str:
    repo = git.Repo(f"{dir_path}/..")
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
    return tags[-1].__str__().strip('v')


def failure_boiler_plate() -> str:
    return (f"\nPlease manually delete the release and tag {get_latest_tag()}, "
            f"fix any issues and try again\n")


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
        raise Exception(f"Tag in CHANGELOG.md ({cl_release}) does not match latest git tag ({tag}). "
                        f"{failure_boiler_plate()}")

    if not date_vals.match(cl_date):
        raise Exception(f"Date ({cl_date}) for this release, does not conform to YYYY-MM-DD pattern. "
                        f"{failure_boiler_plate()}")

    print(f"Release {cl_release} on {cl_date} in CHANGELOG.md OK ✓.")


def calculate_next_version(part=1) -> str:
    tag_parts = re.split(r'[.-]', get_latest_tag().strip("v"))
    tag_seps = re.split(r'[^.|^-]', get_latest_tag().strip("v"))[1:]

    if part == TAG_MAJ:
        print("incrementing major part")
        tag_parts[TAG_MAJ] = str(int(tag_parts[TAG_MAJ]) + 1)
    elif part == TAG_INC:
        print("incrementing incr part")
        tag_parts[TAG_INC] = str(int(tag_parts[TAG_INC]) + 1)
    else:
        print(f"incrementing minor part")
        tag_parts[TAG_MIN] = str(int(tag_parts[TAG_MIN]) + 1)

    return f"{tag_parts[TAG_MAJ]}{tag_seps[TAG_MAJ]}{tag_parts[TAG_MIN]}{tag_seps[TAG_MIN]}{tag_parts[TAG_INC]}"


def update_version():
    version_start_line = "const version = "
    version_locator = re.compile(f"^{version_start_line}.*")
    next_version = calculate_next_version()
    next_version_line = f"{version_start_line}\"{next_version}\"\n"

    with open(VERSION_FILE, "r") as fr:
        lines = fr.readlines()
        for i, line in enumerate(lines):
            if version_locator.match(line):
                lines[i] = next_version_line

    with open(VERSION_FILE, "w+") as fw:
      fw.writelines(lines)

    cl_next_release_line = f"## {next_version} [unreleased]\n"

    with open(CHANGELOG, "r") as fclr:
        cl_lines = fclr.readlines()

    cl_lines.insert(2, cl_next_release_line)
    cl_lines.insert(3, "\n")

    with open(CHANGELOG, "w+") as fclw:
        fclw.writelines(cl_lines)


def upload_next_release_files():
    repo = git.Repo(f"{dir_path}/..")
    with repo.config_writer() as config:
        config.set_value("user","name","karel rehor")
        config.set_value("user","email","karl.koerner@bonitoo.io")

    print(f"DEBUG repo.head.commit           {repo.head.commit}")

    for b in repo.branches:
        print(f"DEBUG branch {b.name}: {b.commit}")

    # TODO following add and commit files...
    # print(f"DEBUG repo.active_branch {repo.active_branch}")
    # repo.index.add(CHANGELOG)
    # repo.index.add(VERSION_FILE)
    # repo.index.commit("chore: prepare for next development iteration [skip ci]")
    # repo.commit(git.Commit())

def inspect():
    os.system("git log -2")


def main():
    print("on-release start")
    print("TODO - under construction")
    verify_changelog()
    update_version()
    upload_next_release_files() # in progress
    inspect()



if __name__ == "__main__":
    main()