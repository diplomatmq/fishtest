#!/usr/bin/env python3
"""Pre-deploy DB compatibility smoke check for SQLite and PostgreSQL.

Usage:
  python scripts/predeploy_db_check.py

Behavior:
- Always runs SQLite smoke-test in an isolated temp DB file.
- Runs PostgreSQL smoke-test only when DATABASE_URL is set.
"""

import os
import sys
import subprocess
from pathlib import Path
from typing import Dict


SMOKE_SNIPPET = r'''
import os
import time
from database import Database

label = os.getenv("DB_SMOKE_LABEL", "unknown")
uid = 900000001
chat_id = -100900000001
username = f"smoke_{label}"

print(f"[SMOKE] backend={label}")

db = Database()

player = db.get_player(uid, chat_id)
if not player:
    player = db.create_player(uid, username, chat_id)

db.update_player(uid, chat_id, coins=int((player or {}).get("coins", 100)) + 1)

_ = db.get_locations()
_ = db.get_rods()

# Referral/star flow compatibility
_ = db.add_ref_access(uid, chat_id)
_ = db.get_ref_access_chats(uid)
_ = db.increment_chat_stars(chat_id, 1, "smoke_chat")
_ = db.get_chat_stars_total(chat_id)
_ = db.get_chat_refunds_total(chat_id)
_ = db.get_available_stars_for_withdraw(uid, chat_id)
_ = db.get_withdrawn_stars(uid, chat_id)
_ = db.mark_stars_withdrawn(uid, 1, chat_id=chat_id)

# Transaction table write/read compatibility
charge_id = f"smoke_{label}_{int(time.time() * 1000)}"
_ = db.add_star_transaction(uid, charge_id, 1, chat_id=chat_id, chat_title="smoke_chat", refund_status="none")
row = db.get_star_transaction(charge_id)
if not row:
    raise RuntimeError("could not read back inserted star transaction")

print(f"[SMOKE] {label}: OK")
'''


def run_smoke(label: str, env_overrides: Dict[str, str]) -> None:
    env = os.environ.copy()
    env.update(env_overrides)
    env["DB_SMOKE_LABEL"] = label

    cmd = [sys.executable, "-c", SMOKE_SNIPPET]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.strip(), file=sys.stderr)
        raise RuntimeError(f"{label} smoke test failed with exit code {result.returncode}")


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    os.chdir(project_root)

    print("[CHECK] Starting pre-deploy DB compatibility checks...")

    # 1) SQLite smoke (always)
    sqlite_path = project_root / "_db_smoke.sqlite"
    if sqlite_path.exists():
        sqlite_path.unlink(missing_ok=True)

    sqlite_env = {
        "DATABASE_URL": "",
        "FISHBOT_DB_PATH": str(sqlite_path),
        "FISHBOT_SKIP_DEFAULT_FILL": "1",
    }
    run_smoke("sqlite", sqlite_env)

    # 2) PostgreSQL smoke (optional)
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        pg_env = {
            "DATABASE_URL": database_url,
            # keep SQLite path irrelevant in PG mode
            "FISHBOT_DB_PATH": str(sqlite_path),
            "FISHBOT_SKIP_DEFAULT_FILL": "1",
        }
        run_smoke("postgres", pg_env)
    else:
        print("[CHECK] DATABASE_URL not set; skipping postgres smoke test.")

    # Cleanup temp sqlite file
    sqlite_path.unlink(missing_ok=True)

    print("[CHECK] All enabled DB checks passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[CHECK] FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)
