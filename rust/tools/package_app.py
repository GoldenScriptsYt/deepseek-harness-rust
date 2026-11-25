"""Build the native desktop app and assemble a portable folder.

    python rust/tools/package_app.py            # release build, then assemble
    python rust/tools/package_app.py --no-build # assemble from the existing exe

Output: rust/dist/DeepSeekHarness/
    DeepSeekHarness.exe        Tauri window + in-process Rust Host (no Node)
    resources/apps/web/dist/   the built web client
    resources/apps/cli/package.json  metadata anchor for the Rust plugin catalog
    resources/packages/...     each boot plugin's package.json, client file and lib/assets

The web client and plugin packages must already be built (`pnpm run build:artifacts`).
This remains a portable folder: Tauri updater installation also requires a real HTTPS
manifest, public verification key, and installer-based bundle, none of which are supplied.
"""
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUST = os.path.dirname(HERE)
REPO = os.path.dirname(RUST)
TAURI = os.path.join(REPO, "apps", "desktop-tauri")
OUT = os.path.join(RUST, "dist", "DeepSeekHarness")
BOOT = os.path.join(RUST, "crates", "dsh-gateway", "assets", "web-boot.captured.json")


def client_file(manifest):
    exp = manifest.get("exports", {}).get("./client")
    if isinstance(exp, dict):
        exp = exp.get("browser") or exp.get("default")
    return (exp or "./lib/client.js").lstrip("./")


def rust_workspace_version():
    section = ""
    with open(os.path.join(RUST, "Cargo.toml"), encoding="utf8") as file:
        for raw in file:
            line = raw.strip()
            if line.startswith("[") and line.endswith("]"):
                section = line
            elif section == "[workspace.package]" and line.startswith("version"):
                value = line.partition("=")[2].split("#", 1)[0].strip()
                return value.strip("\"'")
    raise SystemExit("missing [workspace.package].version in rust/Cargo.toml")


def write_installation_anchor(resources):
    version = rust_workspace_version()
    path = os.path.join(resources, "apps", "cli", "package.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    manifest = {
        "name": "@deepseek-ai/dsh",
        "version": version,
        "private": True,
        "description": "Metadata anchor for the native Rust Host; Node bundles are not installed.",
        "dependencies": {},
        "dsh": {"host": {"profile": "desktop"}},
    }
    with open(path, "w", encoding="utf8") as file:
        json.dump(manifest, file, indent=2)
        file.write("\n")


def main():
    if "--no-build" not in sys.argv:
        subprocess.run(["cargo", "build", "--release", "--locked"], cwd=TAURI, check=True)
    exe = os.path.join(TAURI, "target", "release", "dsh-desktop-tauri.exe")
    if not os.path.isfile(exe):
        sys.exit(f"missing {exe}")

    if os.path.isdir(OUT):
        shutil.rmtree(OUT)
    os.makedirs(OUT)
    shutil.copy2(exe, os.path.join(OUT, "DeepSeekHarness.exe"))

    res = os.path.join(OUT, "resources")
    dist = os.path.join(REPO, "apps", "web", "dist")
    if not os.path.isfile(os.path.join(dist, "index.html")):
        sys.exit("apps/web/dist is not built")
    shutil.copytree(dist, os.path.join(res, "apps", "web", "dist"))
    write_installation_anchor(res)

    wanted = {e["id"] for e in json.load(open(BOOT, encoding="utf8"))["entries"]}
    copied = 0
    packages = os.path.join(REPO, "packages")
    for group in os.listdir(packages):
        gdir = os.path.join(packages, group)
        if not os.path.isdir(gdir):
            continue
        for pkg in os.listdir(gdir):
            src = os.path.join(gdir, pkg)
            manifest_path = os.path.join(src, "package.json")
            if not os.path.isfile(manifest_path):
                continue
            manifest = json.load(open(manifest_path, encoding="utf8"))
            if manifest.get("name") not in wanted:
                continue
            rel = client_file(manifest)
            if not os.path.isfile(os.path.join(src, rel)):
                print("skip (no client build):", manifest["name"])
                continue
            dst = os.path.join(res, "packages", group, pkg)
            os.makedirs(os.path.join(dst, os.path.dirname(rel)), exist_ok=True)
            shutil.copy2(manifest_path, os.path.join(dst, "package.json"))
            shutil.copy2(os.path.join(src, rel), os.path.join(dst, rel))
            assets = os.path.join(src, "lib", "assets")
            if os.path.isdir(assets):
                shutil.copytree(assets, os.path.join(dst, "lib", "assets"))
            copied += 1

    size = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(OUT) for f in fs)
    print(f"packaged {copied}/{len(wanted)} plugin packages -> {OUT} ({size / 1e6:.1f} MB)")


main()
