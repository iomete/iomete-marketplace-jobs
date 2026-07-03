"""
IOMETE dependency smoke-test job.

Purpose: prove that the custom image's Python dependencies (pystardog and the
other packages the real load_constraints script needs) are importable inside an
actual IOMETE Spark job — i.e. on the driver where the job runs.

Run it as a normal IOMETE Python job pointing at your custom image
(iomete/stardog-repro:alokh-latest). If any dependency is missing you'll get the
same ModuleNotFoundError the user is hitting, with a clear line telling you which.
"""

import importlib
import sys
from time import sleep

# Everything the real load_constraints script imports.
# (os, csv, re, datetime are stdlib and always present — kept here for parity.)
MODULES = [
    "stardog",  # provided by pystardog
    "requests",
]


def main():
    print("=" * 60)
    print("Python executable:", sys.executable)
    print("Python version   :", sys.version.replace("\n", " "))
    print("=" * 60)

    failures = []
    for name in MODULES:
        try:
            mod = importlib.import_module(name)
            version = getattr(mod, "__version__", "n/a")
            location = getattr(mod, "__file__", "builtin")
            print(f"OK    {name:12} version={version:10} {location}")
        except Exception as e:
            failures.append(name)
            print(f"FAIL  {name:12} -> {type(e).__name__}: {e}")

    print("=" * 60)
    if failures:
        # Non-zero exit so the IOMETE job is marked FAILED, mirroring the bug.
        print(f"MISSING MODULES: {failures}")
        print(
            "Add the corresponding packages to requirements.txt and rebuild the image."
        )
        sys.exit(1)

    # Prove pystardog is usable, not just importable.
    import stardog

    print(
        "pystardog imported cleanly. Connection object available:",
        hasattr(stardog, "Connection"),
    )
    print("ALL DEPENDENCIES PRESENT — image is good.")


if __name__ == "__main__":
    # sleep(12000)
    main()
