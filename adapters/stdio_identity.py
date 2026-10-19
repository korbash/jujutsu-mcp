#!/usr/bin/env python3
"""Dedicated-executor MCP stdio adapter. Never use one instance for pooled callers.

An editor supplies a fresh, reliable identity file at process launch. This adapter
forwards that identity and remembers generations; the model supplies neither.
"""
import argparse
import json
import os
import subprocess
import sys
import threading


def workspace(path):
    path = os.path.realpath(path)
    while not os.path.isdir(os.path.join(path, ".jj")):
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity-file", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    with open(options.identity_file, encoding="utf-8") as stream:
        identity = json.load(stream)
    required = {"provider", "session_id", "executor_id", "source"}
    if not isinstance(identity, dict) or not required <= set(identity) or set(identity) - required - {"session_name"}:
        parser.error("identity file requires provider, session_id, executor_id and source; session_name is optional")
    if not all(isinstance(value, str) and value.strip() for value in identity.values()):
        parser.error("identity values must be non-empty strings")
    command = options.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        parser.error("backend command is required after --")
    child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8", bufsize=1)
    pending = {}
    generations = {}
    mutex = threading.Lock()

    def responses():
        for line in child.stdout:
            message = json.loads(line)
            with mutex:
                request = pending.pop(message.get("id"), None)
                if request and request[0] == "workspace_lock":
                    result = message.get("result", {}).get("structuredContent", {})
                    if result.get("acquired"):
                        lease = result["lease"]
                        generations[workspace(lease["workspace"])] = lease["generation"]
            sys.stdout.write(line)
            sys.stdout.flush()

    reader = threading.Thread(target=responses, daemon=True)
    reader.start()
    try:
        for line in sys.stdin:
            message = json.loads(line)
            if message.get("method") == "tools/call":
                params = message.setdefault("params", {})
                meta = params.setdefault("_meta", {})
                # Session titles may change while the executor key stays stable.
                forwarded = meta.get("jujutsu-mcp/identity", identity)
                if not isinstance(forwarded, dict) or any(forwarded.get(key) != identity[key] for key in required) or set(forwarded) - required - {"session_name"}:
                    raise ValueError("conflicting forwarded caller identity")
                attached = dict(identity)
                if "session_name" in forwarded:
                    attached["session_name"] = forwarded["session_name"]
                meta["jujutsu-mcp/identity"] = attached
                repo = params.get("arguments", {}).get("repo")
                with mutex:
                    if repo and params.get("name") != "workspace_lock":
                        generation = generations.get(workspace(repo))
                        if generation is not None:
                            meta["jujutsu-mcp/generation"] = generation
                    pending[message.get("id")] = (params.get("name"), repo)
            child.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
            child.stdin.flush()
    finally:
        child.stdin.close()
        reader.join()
        child.wait()
    return child.returncode


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError):
        # No input, prompts, credentials or entire request bodies in diagnostics.
        print("workspace adapter failed: check identity configuration and backend transport", file=sys.stderr)
        sys.exit(1)
