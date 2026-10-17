"""Check that the sdk-streams pin and the sdk-core pin name the same sdk-rust.

The bridge patches the crate's sdk-rust dependencies onto the submodule, so the build never
reads the sdk-rust rev in the sdk-streams manifest. This check fails when that rev and the
submodule commit drift apart, or when the patch does not apply and Cargo builds a second
copy of the sdk-rust crates.
"""

import json
import subprocess
import sys
from pathlib import Path

import tomllib

base_dir = Path(__file__).parent.parent
bridge_manifest = base_dir / "temporalio" / "bridge" / "Cargo.toml"
patched_crates = {"temporalio-common", "temporalio-client"}


def main() -> int:
    metadata = json.loads(
        subprocess.check_output(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--manifest-path",
                str(bridge_manifest),
            ]
        )
    )
    streams = [p for p in metadata["packages"] if p["name"] == "temporalio-streams"]
    if len(streams) != 1:
        print(f"expected one temporalio-streams package, found {len(streams)}")
        return 1
    unpatched = [
        f"{p['name']} {p['source']}"
        for p in metadata["packages"]
        if p["name"] in patched_crates and p["source"] is not None
    ]
    if unpatched:
        print(f"the sdk-rust patch did not apply to: {', '.join(unpatched)}")
        return 1

    # The crate's manifest sits at crates/streams in the sdk-streams workspace.
    workspace = Path(streams[0]["manifest_path"]).parents[2] / "Cargo.toml"
    deps = tomllib.loads(workspace.read_text())["workspace"]["dependencies"]
    revs = {name: deps[name]["rev"] for name in patched_crates if name in deps}
    submodule = subprocess.check_output(
        ["git", "rev-parse", "HEAD:temporalio/bridge/sdk-core"], cwd=base_dir, text=True
    ).strip()
    drift = {name: rev for name, rev in revs.items() if rev != submodule}
    if not revs or drift:
        print(
            f"sdk-streams pins sdk-rust {revs or 'nowhere'}, sdk-core is at {submodule}"
        )
        return 1
    print(f"sdk-streams and sdk-core both pin sdk-rust {submodule}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
