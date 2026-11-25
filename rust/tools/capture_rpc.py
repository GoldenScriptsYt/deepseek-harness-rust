"""Record the TypeScript Host's answer to RPC calls as fixtures for the Rust Host.

Start the reference Host (`node --import tsx/esm apps/cli/src/bin.ts web --no-open --port 3191`
with its own DSH_HOME), sign in once so the cookie jar exists:

    curl -s -c $TEMP/jar.txt -o /dev/null "http://127.0.0.1:3191/?token=<token>"

then run, giving method/payload pairs (payload is the full RPC payload JSON):

    python capture_rpc.py session/create '{"args":{"request":{"workspaceId":"..."}}}'

Responses land in rust/crates/dsh-cli/assets/rpc/<method with / replaced by _>.json.
Only the `value` of a successful result is stored.
"""
import json
import os
import subprocess
import sys

TEMP = os.environ.get("TEMP", "/tmp")
JAR = os.environ.get("DSH_REF_JAR", TEMP + "/jar.txt")
ORIGIN = os.environ.get("DSH_REF_ORIGIN", "http://127.0.0.1:3191")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "crates", "dsh-cli", "assets", "rpc")

args = sys.argv[1:]
if not args or len(args) % 2:
    sys.exit(__doc__)
for method, payload in zip(args[::2], args[1::2]):
    body = json.dumps({"type": "client-request", "rpcId": "cap", "method": method, "payload": json.loads(payload)})
    tmp = os.path.join(TEMP, "cap.json")
    subprocess.run(["curl", "-s", "-o", tmp, "-b", JAR, "-X", "POST", f"{ORIGIN}/api/{method}",
                    "-H", "content-type: application/json", "-d", body], check=True)
    raw = open(tmp, "rb").read().decode("utf8", "replace")
    try:
        res = json.loads(raw)["result"]
    except Exception:
        print("BAD", method, raw[:150])
        continue
    if res.get("ok"):
        name = method.replace("/", "_") + ".json"
        json.dump(res["value"], open(os.path.join(OUT, name), "w", encoding="utf8"), ensure_ascii=False, indent=1)
        print("OK ", method, len(json.dumps(res["value"])), "bytes ->", name)
    else:
        print("ERR", method, json.dumps(res.get("error"))[:240])
