"""Generates the Stream service bindings of temporalio.contrib.streams.

The WIT and the protos are copies of the temporalio-streams crate's, in scripts/_wit and
scripts/_proto. Run scripts/gen_protos.py first for the proto modules. No nexgen release has the
offset, long-poll and client features yet, so the script installs nexgen at
NEX_GEN_REVISION from moetemp/nexgen. NEX_GEN_STREAMS_BIN may name a build of that revision
instead.
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
proto_path = (
    gen_protos.additional_proto_dir
    / "temporal"
    / "api"
    / "streamservice"
    / "v1"
    / "stream_service.proto"
)
output_dir = base_dir / "temporalio" / "contrib" / "streams" / "system"
# The writer is client-only, so it has a caller and its resource class, and no Workflow code.
writer_output_dir = base_dir / "temporalio" / "contrib" / "streams" / "writer_system"

NEX_GEN_REPOSITORY = "https://github.com/moetemp/nexgen"
# The head of moe/brook/v3/n8-stream-resource. A branch moves, so the pin is the commit.
NEX_GEN_REVISION = "204b566ee996939b165ccd1d18c8c432bfdcc0dd"
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
        # The extension messages live in temporalio.contrib.streams.proto, so the descriptor
        # names their file by that module path.
        extension_root = Path(temp_dir) / "extension"
        extension_path = (
            extension_root
            / "temporalio"
            / "contrib"
            / "streams"
            / "proto"
            / "v1"
            / "extension.proto"
        )
        extension_path.parent.mkdir(parents=True)
        shutil.copy(
            gen_protos.additional_proto_dir
            / "temporal"
            / "sdk"
            / "streams"
            / "v1"
            / "extension.proto",
            extension_path,
        )
        subprocess.check_call(
            [
                sys.executable,
                "-mgrpc_tools.protoc",
                f"--proto_path={gen_protos.api_proto_dir}",
                f"--proto_path={gen_protos.additional_proto_dir}",
                f"--proto_path={extension_root}",
                "--include_imports",
                f"--descriptor_set_out={descriptor_path}",
                str(proto_path),
                str(extension_path),
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
                "--client",
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
                "--native-api",
                "--client",
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
    print(
        f"Generating the Stream service bindings with nexgen {NEX_GEN_REPOSITORY}"
        f"@{NEX_GEN_REVISION}...",
        file=sys.stderr,
    )
    generate_stream_service()
    print("Done", file=sys.stderr)
