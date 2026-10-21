#!/usr/bin/env python3
"""Fails a PR whose job VERSION cannot be released: not image:1.2.15, a renamed image,
not higher, or already published. A job is any folder with a VERSION file.

usage: check_versions.py <base-ref>
"""
import glob
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

VERSION = re.compile(r"^([a-z0-9_.-]+):(\d+\.\d+\.\d+)$")


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


def problem(base: str, folder: str) -> str | None:
    line = open(f"{folder}/VERSION").read().strip()
    before = git("show", f"{base}:{folder}/VERSION")
    if line == before:
        if git("diff", "--name-only", f"{base}...HEAD", "--", folder):
            print(f"::warning::{folder} changed but VERSION is still {line}, so merging releases nothing")
        return None
    match = VERSION.match(line)
    if not match:
        return f"{folder}/VERSION: '{line}' is not <image>:<version> like iomete_data_compaction:1.2.15"
    if not os.path.exists(f"{folder}/Dockerfile"):
        return f"{folder}: a job needs a Dockerfile next to its VERSION"
    image, version = match.groups()
    if before:
        old_image, _, old_version = before.partition(":")
        if image != old_image:
            return f"{folder}: VERSION renames the image from {old_image} to {image}"
        if [int(p) for p in version.split(".")] <= [int(p) for p in old_version.split(".")]:
            return f"{folder}: VERSION {version} is not higher than {old_version}"
        # A job's first VERSION may name a version already shipped by hand.
        if published(image, version):
            return f"{folder}: {image}:{version} is already published, pick a new version"
    print(f"{image}: {before.partition(':')[2] or 'new'} -> {version}")
    return None


if __name__ == "__main__":
    folders = sorted(os.path.dirname(path) for path in glob.glob("*/VERSION"))
    problems = [p for folder in folders if (p := problem(sys.argv[1], folder))]
    for p in problems:
        print(f"::error::{p}")
    sys.exit(1 if problems else 0)
