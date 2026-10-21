#!/usr/bin/env python3
"""Checks the jobs in marketplace-jobs.json against the PR's base, before anything is released.

A changed VERSION must be a new, higher version that the registry does not hold yet,
except a job's first VERSION, which may adopt a version already shipped.
A job whose files changed without a VERSION bump gets a warning, since merging it
releases nothing.

usage: check_versions.py <base-ref>
"""
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

MANIFEST = "marketplace-jobs.json"
REGISTRY = "iomete.azurecr.io"
NAMESPACE = "iomete"
VERSION = re.compile(r"^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$")
REQUIRED = ("dir", "dockerfile", "context")


def at_base(base: str, path: str) -> str | None:
    result = subprocess.run(["git", "show", f"{base}:{path}"], capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else None


def changed(base: str, path: str) -> bool:
    diff = subprocess.run(["git", "diff", "--name-only", f"{base}...HEAD", "--", path],
                          capture_output=True, text=True, check=True)
    return bool(diff.stdout.strip())


def version_key(version: str) -> tuple:
    core, _, pre = version.partition("-")
    # A pre-release sorts below its release, as in semver.
    return tuple(int(p) for p in core.split(".")), pre == "", pre


def published(name: str, version: str) -> bool:
    scope = urllib.parse.quote(f"repository:{NAMESPACE}/{name}:pull")
    with urllib.request.urlopen(f"https://{REGISTRY}/oauth2/token?service={REGISTRY}&scope={scope}", timeout=30) as resp:
        token = json.loads(resp.read())["access_token"]
    req = urllib.request.Request(f"https://{REGISTRY}/v2/{NAMESPACE}/{name}/manifests/{version}", method="HEAD")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", ", ".join([
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]))
    try:
        with urllib.request.urlopen(req, timeout=30):
            return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def check(base: str) -> list[str]:
    errors = []
    manifest = json.load(open(MANIFEST))
    for name, job in manifest.items():
        missing = [k for k in REQUIRED if k not in job]
        if missing:
            errors.append(f"{name}: missing {', '.join(missing)} in {MANIFEST}")
            continue
        for key in REQUIRED:
            if not os.path.exists(job[key]):
                errors.append(f"{name}: {key} {job[key]} does not exist")
        version_file = f"{job['dir']}/VERSION"
        if not os.path.exists(version_file):
            errors.append(f"{name}: {version_file} does not exist")
            continue

        version = open(version_file).read().strip()
        before = (at_base(base, version_file) or "").strip()
        if version == before:
            if changed(base, job["dir"]):
                print(f"::warning file={version_file}::{name} changed but VERSION is still {version}, so merging releases nothing")
            continue
        if not VERSION.match(version):
            errors.append(f"{name}: VERSION '{version}' is not a version like 1.2.15")
            continue
        if before and VERSION.match(before) and version_key(version) <= version_key(before):
            errors.append(f"{name}: VERSION {version} is not higher than {before}")
            continue
        if published(name, version):
            # A first VERSION may name what was already shipped by hand: merging only tags it.
            if not before:
                print(f"{name}: adopts the published {version}, merging tags it")
                continue
            errors.append(f"{name}: {version} is already published, pick a new version")
            continue
        print(f"{name}: {before or 'new'} -> {version}, released on merge")
    return errors


if __name__ == "__main__":
    problems = check(sys.argv[1])
    for problem in problems:
        print(f"::error::{problem}")
    sys.exit(1 if problems else 0)
