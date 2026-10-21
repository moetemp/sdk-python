"""Generates the Stream service bindings of temporalio.contrib.streams.

The WIT and the protos are copies of the temporalio-streams crate's, in scripts/_wit and
scripts/_proto. Run scripts/gen_protos.py first for the proto modules. No nexgen release has the
offset, one-way, long-poll, handle and client features yet, so NEX_GEN_STREAMS_BIN must name an
advanced build of moetemp/nexgen moe/brook/v3/n9-handles.
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
    gen_protos.additional_proto_dir
    / "temporal"
    / "api"
    / "streamservice"
    / "v1"
    / "stream_service.proto"
)
output_dir = base_dir / "temporalio" / "contrib" / "streams" / "system"


def nex_gen_command() -> list[str]:
    bin_path = os.environ.get("NEX_GEN_STREAMS_BIN")
    if not bin_path:
        raise RuntimeError("set NEX_GEN_STREAMS_BIN to a nexgen build with --client")
    return [bin_path]


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
                f"--proto_path={gen_protos.additional_proto_dir}",
                "--include_imports",
                f"--descriptor_set_out={descriptor_path}",
                str(proto_path),
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
    print("Generating the Stream service bindings...", file=sys.stderr)
    generate_stream_service()
    print("Done", file=sys.stderr)
