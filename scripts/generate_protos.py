#!/usr/bin/env python3
import os
import subprocess
import sys


def main():
    # Ensure the output directory exists
    os.makedirs("src/grpc/generated", exist_ok=True)

    # Create an empty __init__.py file to make the directory a package
    with open("src/grpc/generated/__init__.py", "w") as f:
        f.write("# This file is generated to make the directory a package\n")

    # Run the protoc compiler
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        "-I./protos",
        "--python_out=./src/grpc/generated",
        "--grpc_python_out=./src/grpc/generated",
        "./protos/peer_edge.proto"
    ]

    result = subprocess.run(cmd, capture_output=True)

    if result.returncode != 0:
        print("Error generating proto files:")
        print(result.stderr.decode())
        return 1

    print("Successfully generated proto files!")

    # Fix imports in the generated files
    fix_imports("src/grpc/generated/peer_edge_pb2_grpc.py")

    return 0


def fix_imports(file_path):
    """Fix relative imports in the generated grpc file."""
    with open(file_path, "r") as f:
        content = f.read()

    # Fix the import statement
    content = content.replace("import peer_edge_pb2", "from . import peer_edge_pb2")

    with open(file_path, "w") as f:
        f.write(content)


if __name__ == "__main__":
    sys.exit(main())
