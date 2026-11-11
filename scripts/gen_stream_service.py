"""Generates the Stream service bindings of the Streams API in temporalio.streams.

The WIT in scripts/_wit is a byte-for-byte copy of the API repo's, and the Stream service protos
are in the API protos of the Core submodule. Run scripts/gen_protos.py first for the proto
modules. No nexgen release has the directives of the WIT, so the script installs nexgen at
NEX_GEN_REVISION. Set NEX_GEN_STREAMS_BIN to use a local build of that revision instead.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import gen_protos

base_dir = Path(__file__).parent.parent
wit_path = base_dir / "scripts" / "_wit" / "stream-service.wit"
model_wit_path = (
    gen_protos.api_proto_dir / "nexus" / "deps" / "nexus-temporal-types" / "model.wit"
)
proto_path = (
    gen_protos.api_proto_dir
    / "temporal"
    / "api"
    / "streamservice"
    / "v1"
    / "request_response.proto"
)
output_dir = base_dir / "temporalio" / "streams" / "system"

NEX_GEN_REPOSITORY = "https://github.com/temporalio/nexgen"
# A revision that reads the directives of the WIT.
NEX_GEN_REVISION = "bbf5c1595a703563ff373d507d6e8174144cdf7c"
nex_gen_root = base_dir / ".nexgen-streams" / NEX_GEN_REVISION


def nex_gen_command() -> list[str]:
    if bin_path := os.environ.get("NEX_GEN_STREAMS_BIN"):
        return [bin_path]
    binary = nex_gen_root / "bin" / "nexgen"
    if not binary.exists():
        subprocess.check_call(
            [
                "cargo",
                "install",
                "--locked",
                "nexgen",
                "--git",
                NEX_GEN_REPOSITORY,
                "--rev",
                NEX_GEN_REVISION,
                "--features",
                "advanced",
                "--root",
                str(nex_gen_root),
            ],
            # The git CLI honours the user's URL rewrites and credentials, which cargo's
            # built-in client does not.
            env={**os.environ, "CARGO_NET_GIT_FETCH_WITH_CLI": "true"},
        )
    return [str(binary)]


def api_protos() -> list[Path]:
    return sorted(
        path
        for path in (gen_protos.api_proto_dir / "temporal" / "api").glob("**/*.proto")
        if "cloud" not in path.parts and "operatorservice" not in path.parts
    )


def generate_stream_service() -> None:
    for path in (wit_path, model_wit_path, proto_path):
        if not path.exists():
            raise RuntimeError(f"missing generator input: {path}")
    with tempfile.TemporaryDirectory(dir=base_dir) as temp_dir:
        descriptor_path = Path(temp_dir) / "stream_service.bin"
        subprocess.check_call(
            [
                sys.executable,
                "-mgrpc_tools.protoc",
                f"--proto_path={gen_protos.api_proto_dir}",
                "--include_imports",
                f"--descriptor_set_out={descriptor_path}",
                # The Stream service protos and the API types of the model WIT are all API
                # protos, so their descriptors are in the set.
                *map(str, api_protos()),
            ]
        )
        shutil.rmtree(output_dir, ignore_errors=True)
        subprocess.check_call(
            [
                *nex_gen_command(),
                "python",
                str(wit_path),
                str(model_wit_path),
                "--system-nexus",
                "--targets",
                "workflow,client",
                "--support-package",
                "temporalio.nexus.system._support",
                "--descriptors",
                str(descriptor_path),
                "--output",
                str(output_dir),
            ]
        )
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--select",
            "I",
            "--fix",
            str(output_dir),
        ]
    )
    subprocess.check_call([sys.executable, "-m", "ruff", "format", str(output_dir)])


if __name__ == "__main__":
    if sys.argv[1:] == ["--install-nexgen"]:
        # CI runs the install as its own step, so an install failure shows apart from drift.
        nex_gen_command()
        sys.exit()
    print(
        f"Generating the Stream service bindings with nexgen {NEX_GEN_REPOSITORY}"
        f"@{NEX_GEN_REVISION}...",
        file=sys.stderr,
    )
    generate_stream_service()
    print("Done", file=sys.stderr)
