"""Generate the stream service's Python bindings from its Nexus contract.

The stream runtime serves the contract, so its one copy is the
``crates/streams/nexus/temporal_streams.nexusrpc.yaml`` of the temporalio-streams
crate that the bridge pins, and the bindings land in
``temporalio/contrib/streams/nexus/_generated``. With
``--check`` the script generates into a scratch directory and fails when the
result differs from what is checked in, which is how CI keeps the two in step.

The contract uses the ``x-nexus-long-poll``, ``x-nexus-cursor``,
``x-nexus-stream-ref``, ``x-nexus-handle`` and ``x-nexus-operation-token``
annotations, and the bindings include the forwarding handler, which no nexgen
release carries yet. So it uses the nexgen fork revision that
``gen_nexus_system_api.py`` pins, and the same install.
"""

import argparse
import filecmp
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import gen_protos
from gen_nexus_system_api import nex_gen_command

base_dir = Path(__file__).parent.parent
package_dir = base_dir / "temporalio" / "contrib" / "streams" / "nexus"
contract_path = (
    gen_protos.streams_proto_dir.parent / "nexus" / "temporal_streams.nexusrpc.yaml"
)
output_dir = package_dir / "_generated"


def generate(target: Path) -> None:
    """Generate the bindings into ``target``, formatted as the repository formats."""
    subprocess.check_call(
        [
            *nex_gen_command(),
            "python",
            "--client",
            "--forwarder",
            "--output",
            str(target),
            str(contract_path),
        ]
    )
    for fix in (["check", "--select", "I", "--fix"], ["format"]):
        subprocess.check_call(
            [sys.executable, "-m", "ruff", *fix, "--quiet", str(target)]
        )


def differences(left: Path, right: Path) -> list[str]:
    """The relative paths whose content differs between two trees."""
    comparison = filecmp.dircmp(left, right, ignore=["__pycache__"])
    found = [
        *comparison.left_only,
        *comparison.right_only,
        *comparison.diff_files,
        *comparison.funny_files,
    ]
    for name, sub in comparison.subdirs.items():
        found.extend(
            f"{name}/{path}" for path in differences(Path(sub.left), Path(sub.right))
        )
    _, mismatch, errors = filecmp.cmpfiles(
        left, right, comparison.same_files, shallow=False
    )
    return sorted({*found, *mismatch, *errors})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail when regenerating would change the checked-in bindings",
    )
    args = parser.parse_args()
    if not contract_path.exists():
        raise RuntimeError(f"missing contract: {contract_path}")

    with tempfile.TemporaryDirectory(dir=base_dir) as temp_dir:
        scratch = Path(temp_dir) / "_generated"
        generate(scratch)
        if args.check:
            changed = differences(scratch, output_dir) if output_dir.exists() else ["."]
            if changed:
                print(
                    "The stream service bindings are stale; run "
                    "`uv run scripts/gen_nexus_streams_api.py`. Changed: "
                    + ", ".join(changed),
                    file=sys.stderr,
                )
                sys.exit(1)
            return
        shutil.rmtree(output_dir, ignore_errors=True)
        shutil.copytree(scratch, output_dir)


if __name__ == "__main__":
    main()
