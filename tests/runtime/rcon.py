#!/usr/bin/env python3
"""Minimal GoldSrc UDP RCON client used only inside the isolated test network."""

from __future__ import annotations

import argparse
import socket

PREFIX = b"\xff\xff\xff\xff"


def exchange(host: str, port: int, payload: bytes, timeout: float) -> bytes:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
        client.settimeout(timeout)
        client.sendto(PREFIX + payload + b"\n", (host, port))
        response, _ = client.recvfrom(65535)
    if not response.startswith(PREFIX):
        raise RuntimeError("RCON response did not contain the GoldSrc prefix")
    return response[len(PREFIX) :]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--host", default="hlds")
    parser.add_argument("--port", type=int, default=27015)
    parser.add_argument("--password", default="runtime-only-password")
    parser.add_argument("--timeout", type=float, default=3.0)
    args = parser.parse_args()

    challenge_response = exchange(
        args.host, args.port, b"challenge rcon", args.timeout
    ).decode("latin-1", errors="replace")
    parts = challenge_response.strip().split()
    if len(parts) < 3 or parts[0:2] != ["challenge", "rcon"]:
        raise RuntimeError(f"Unexpected RCON challenge: {challenge_response!r}")

    challenge = parts[2]
    command = f'rcon {challenge} "{args.password}" {args.command}'.encode("latin-1")
    print(exchange(args.host, args.port, command, args.timeout).decode("latin-1", errors="replace"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
