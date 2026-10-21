#!/usr/bin/env python3
"""Fails a PR whose job VERSION cannot be released: not a version, not higher, or already published.

usage: check_versions.py <base-ref>
"""
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request

VERSION = re.compile(r"^\d+\.\d+\.\d+$")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True).stdout.strip()


def published(name: str, version: str) -> bool:
    """Asked anonymously, as a customer pulls. Anything but a found or missing tag fails the check."""
    registry, repo = "https://iomete.azurecr.io", f"iomete/{name}"
    with urllib.request.urlopen(f"{registry}/oauth2/token?service=iomete.azurecr.io&scope=repository:{repo}:pull") as r:
        token = json.load(r)["access_token"]
    request = urllib.request.Request(f"{registry}/v2/{repo}/manifests/{version}", method="HEAD", headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, "
                  "application/vnd.docker.distribution.manifest.list.v2+json, "
                  "application/vnd.docker.distribution.manifest.v2+json",
    })
    try:
        urllib.request.urlopen(request)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def problem(base: str, name: str, folder: str) -> str | None:
    version = open(f"{folder}/VERSION").read().strip()
    before = git("show", f"{base}:{folder}/VERSION")
    if version == before:
        if git("diff", "--name-only", f"{base}...HEAD", "--", folder):
            print(f"::warning::{name} changed but VERSION is still {version}, so merging releases nothing")
        return None
    if not VERSION.match(version):
        return f"{name}: VERSION '{version}' is not a version like 1.2.15"
    if before and [int(p) for p in version.split(".")] <= [int(p) for p in before.split(".")]:
        return f"{name}: VERSION {version} is not higher than {before}"
    # A job's first VERSION may name a version already shipped by hand.
    if before and published(name, version):
        return f"{name}: {version} is already published, pick a new version"
    print(f"{name}: {before or 'new'} -> {version}")
    return None


if __name__ == "__main__":
    jobs = json.load(open("marketplace-jobs.json"))
    problems = [p for name, job in jobs.items() if (p := problem(sys.argv[1], name, job["dir"]))]
    for p in problems:
        print(f"::error::{p}")
    sys.exit(1 if problems else 0)
