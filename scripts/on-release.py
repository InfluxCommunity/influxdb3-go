#!/usr/bin/env python
import os
import re

import git

dir_path = os.path.dirname(os.path.realpath(__file__))
CHANGELOG = f"{dir_path}/../CHANGELOG.md"
VERSION_FILE = f"{dir_path}/../influxdb3/version.go"
BRANCH_NEXT_SUB_TOKEN = "chore/prepare-next-release-"

TAG_MAJ = 0
TAG_MIN = 1
TAG_INC = 2


def get_trigger_tag() -> str:
    return os.environ['CIRCLE_TAG']


def failure_boiler_plate() -> str:
    return (f"\nPlease manually delete the release and tag {get_trigger_tag()}, "
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

    tag = get_trigger_tag().strip('v')
    if cl_release != tag:
        raise Exception(f"Tag in CHANGELOG.md ({cl_release}) does not match latest git tag ({tag}). "
                        f"{failure_boiler_plate()}")

    if not date_vals.match(cl_date):
        raise Exception(f"Date ({cl_date}) for this release, does not conform to YYYY-MM-DD pattern. "
                        f"{failure_boiler_plate()}")

    print(f"Release {cl_release} on {cl_date} in CHANGELOG.md OK ✓.")


def verify_version_file():
    tag = get_trigger_tag().strip("v")
    version_pattern = re.compile("^const version =.* ")
    with open(VERSION_FILE, "r") as f:
        lines = f.readlines()
        for line in lines:
            if version_pattern.match(line):
                version = line.split("\"")[1]
                if tag != version:
                    raise Exception(f"Version in {VERSION_FILE} file ({version}) does not match latest tag ({tag})"
                                    f"{failure_boiler_plate()}")
                else:
                    print(f"Version ({version}) in version file matches current tag ({tag})")
                    return
        raise Exception(f"Failed to locate version line {version_pattern.pattern} in {VERSION_FILE}"
                        f"{failure_boiler_plate()}")


def calculate_next_version(part=1) -> str:
    tag_control = re.compile(r"^\d+\.\d+\.\d+.*")
    latest_tag = get_trigger_tag().strip("v")

    if not tag_control.match(latest_tag):
        raise Exception(f"Latest tag {latest_tag} does match control pattern {tag_control.pattern}. Cannot update.")

    tag_parts = []
    tag_seps = []
    part_index = 0

    for i, c in enumerate(latest_tag):
        if c == ".":
            tag_parts.append(latest_tag[part_index:i])
            tag_seps.append(latest_tag[i])
            part_index = i + 1

    tag_parts.append(latest_tag[part_index:])

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
        config.set_value("user", "name", "builder")
        config.set_value("user", "email", "builder@bonitoo.io")

    next_version = calculate_next_version()

    targetBranchName = f"{BRANCH_NEXT_SUB_TOKEN}{next_version}"

    target_branch = repo.create_head(targetBranchName)

    print(f"Switching to branch {target_branch}")
    repo.head.reference = target_branch

    repo.index.add(CHANGELOG)
    repo.index.add(VERSION_FILE)
    repo.index.commit("chore: prepare for next development iteration [skip ci]")

    repo.git.push("--set-upstream", "origin", targetBranchName)
    repo.git.request_pull(targetBranchName, repo.remotes.origin.url, "main")

    print(f"Changes for next release {next_version} are in the new branch {target_branch}.  "
          f"Please review them and merge them into main.")


def main():
    print("on-release start")
    verify_changelog()
    verify_version_file()
    update_version()
    upload_next_release_files()


if __name__ == "__main__":
    main()
