#!/usr/bin/env python3
import argparse
import os
import sys
import time

import requests


def login(base_url, user, password):
    resp = requests.post(
        f"{base_url}/api/auth/login",
        json={"username": user, "password": password},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def list_devices(base_url, token, prefix):
    headers = {"X-Authorization": f"Bearer {token}"}
    page = 0
    while True:
        resp = requests.get(
            f"{base_url}/api/tenant/devices",
            params={
                "pageSize": 100,
                "page": page,
                "textSearch": prefix,
                "sortProperty": "name",
                "sortOrder": "ASC",
            },
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        for device in payload.get("data", []):
            name = device.get("name", "")
            if name.startswith(prefix):
                yield device
        if not payload.get("hasNext"):
            break
        page += 1


def delete_device(base_url, token, device_id):
    headers = {"X-Authorization": f"Bearer {token}"}
    resp = requests.delete(f"{base_url}/api/device/{device_id}", headers=headers, timeout=20)
    resp.raise_for_status()


def main():
    parser = argparse.ArgumentParser(description="Delete devices by exact name prefix.")
    parser.add_argument("--prefix", default=os.environ.get("DEVICE_PREFIX", "thingsflow-test-"))
    parser.add_argument("--base-url", default=os.environ.get("TB_BASE_URL", "https://thingsflow.example.com"))
    parser.add_argument("--user", default=os.environ.get("TB_USER", "tenant@thingsboard.org"))
    parser.add_argument("--password", default=os.environ.get("TB_PASS", "tenant"))
    parser.add_argument("--execute", action="store_true", help="Actually delete devices. Default is dry-run.")
    parser.add_argument("--sleep", type=float, default=0.05, help="Seconds to wait between deletes.")
    args = parser.parse_args()

    if len(args.prefix) < 8:
        print("Refusing short prefix. Use a specific device prefix.", file=sys.stderr)
        return 2

    token = login(args.base_url, args.user, args.password)
    devices = list(list_devices(args.base_url, token, args.prefix))
    print(f"Matched {len(devices)} device(s) with prefix {args.prefix!r}")

    for device in devices:
        device_id = device["id"]["id"]
        name = device["name"]
        if args.execute:
            delete_device(args.base_url, token, device_id)
            print(f"deleted {name} {device_id}")
            time.sleep(args.sleep)
        else:
            print(f"dry-run {name} {device_id}")

    if not args.execute:
        print("Dry run only. Re-run with --execute to delete.")


if __name__ == "__main__":
    raise SystemExit(main())
