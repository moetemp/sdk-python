"""Generates the Stream service bindings of the Streams API in temporalio.common.

The WIT and the runtime proto are copies of the temporalio-streams crate's, in scripts/_wit and
scripts/_proto. The Stream service protos are copies of the API repo's. Run scripts/gen_protos.py
first for the proto modules. No nexgen release has the client target and the poll directive,
so the script installs nexgen at NEX_GEN_REVISION from moetemp/nexgen. Set NEX_GEN_STREAMS_BIN
to use a local build of that revision instead.
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
writer_wit_path = base_dir / "scripts" / "_wit" / "stream-writer.wit"
model_wit_path = (
    gen_protos.api_proto_dir / "nexus" / "deps" / "nexus-temporal-types" / "model.wit"
)
# It imports the Stream messages and enums, so --include_imports puts them in the descriptor.
proto_path = (
    gen_protos.additional_proto_dir
    / "temporal"
    / "api"
    / "streamservice"
    / "v1"
    / "request_response.proto"
)
output_dir = base_dir / "temporalio" / "common" / "_streams" / "system"
# Only Activities and clients call the writer service, so its bindings have no Workflow code.
writer_output_dir = base_dir / "temporalio" / "common" / "_streams" / "writer_system"

NEX_GEN_REPOSITORY = "https://github.com/moetemp/nexgen"
# The fork chain of nexgen with the client target and the poll directive.
NEX_GEN_REVISION = "d70f5f1754c2e694b859f4a38b24c73fab27d8da"
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
    for path in (wit_path, writer_wit_path, model_wit_path, proto_path):
        if not path.exists():
            raise RuntimeError(f"missing generator input: {path}")
    with tempfile.TemporaryDirectory(dir=base_dir) as temp_dir:
        descriptor_path = Path(temp_dir) / "stream_service.bin"
        # The generated runtime messages are in temporalio.common._streams.proto, so the
        # descriptor must name their file by that module path.
        runtime_root = Path(temp_dir) / "runtime"
        runtime_path = (
            runtime_root
            / "temporalio"
            / "common"
            / "_streams"
            / "proto"
            / "v1"
            / "runtime.proto"
        )
        runtime_path.parent.mkdir(parents=True)
        shutil.copy(
            gen_protos.additional_proto_dir
            / "temporal"
            / "sdk"
            / "streams"
            / "v1"
            / "runtime.proto",
            runtime_path,
        )
        subprocess.check_call(
            [
                sys.executable,
                "-mgrpc_tools.protoc",
                f"--proto_path={gen_protos.api_proto_dir}",
                f"--proto_path={gen_protos.additional_proto_dir}",
                f"--proto_path={runtime_root}",
                "--include_imports",
                f"--descriptor_set_out={descriptor_path}",
                str(proto_path),
                str(runtime_path),
                # The model WIT names API types, so their descriptors must be in the set.
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
                "--native-api",
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
        shutil.rmtree(writer_output_dir, ignore_errors=True)
        subprocess.check_call(
            [
                *nex_gen_command(),
                "python",
                str(writer_wit_path),
                str(wit_path),
                str(model_wit_path),
                "--system-nexus",
                "--native-api",
                "--targets",
                "client",
                "--support-package",
                "temporalio.nexus.system._support",
                "--descriptors",
                str(descriptor_path),
                "--output",
                str(writer_output_dir),
            ]
        )
    for directory in (output_dir, writer_output_dir):
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "ruff",
                "check",
                "--select",
                "I",
                "--fix",
                str(directory),
            ]
        )
        subprocess.check_call([sys.executable, "-m", "ruff", "format", str(directory)])


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
