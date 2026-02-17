"""
Script: calc_expected_xp.py
Usage: edit USER_ID at top and run `python tools/calc_expected_xp.py` from project root.

This script computes the expected XP for a user based on sold caught_fish entries
and compares it to the player's stored XP in the `players` table.

It uses the project's `database.db` instance and calculation helpers.
"""
from typing import List
import pprint

# Change this ID to the target user
USER_ID = 7855666356
# chat_id used for lookups; set -1 to match global rows (code treats <1 as global)
from typing import List
import pprint
import os
import sys
from pathlib import Path

# Change this ID to the target user
USER_ID = 7855666356
# chat_id used for lookups:
# - set to -1 to match global rows (code treats <1 as global)
# - set to None or 'ANY' to include caught_fish from all chats for the user
CHAT_ID = None


def ensure_project_on_path():
    """Add project root to sys.path so imports like `from database import db` work.

    This script is intended to be run from the project root or from the container
    where the bot runs. If run elsewhere, attempt to locate the repo by walking
    up until we find `bot.py` or `database.py`.
    """
    cwd = Path.cwd()
    for p in [cwd] + list(cwd.parents):
        if (p / 'bot.py').exists() or (p / 'database.py').exists():
            sys.path.insert(0, str(p))
            return
    # fallback: insert script's parent/.. (assume tools/ inside project)
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


if __name__ == '__main__':
    ensure_project_on_path()

    # Ensure DATABASE_URL is present for Postgres usage (matches bot behavior)
    if not os.getenv('DATABASE_URL') and not os.getenv('FISHBOT_DB_PATH'):
        print("Warning: neither DATABASE_URL nor FISHBOT_DB_PATH set. The bot uses Postgres on server; set DATABASE_URL to connect.")

    # Import here to allow running as standalone script from project root
    try:
        from database import db
    except Exception as e:
        print('Failed to import project `database` module:', e)
        raise

    pp = pprint.PrettyPrinter(indent=2)

    print(f"Inspecting user: {USER_ID} (chat_id lookup: {CHAT_ID})\n")

    # Fetch all caught fish visible to the user (read-only)
    # If CHAT_ID is None or 'ANY', fetch across all chats; otherwise use
    # the project's `get_caught_fish` helper which applies chat-aware filtering.
    if CHAT_ID is None or CHAT_ID == 'ANY':
        with db._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT cf.*, 
                       COALESCE(f.name, t.name) AS name,
                       COALESCE(f.rarity, 'Мусор') AS rarity,
                       COALESCE(f.price, t.price, 0) AS price,
                       f.min_weight AS min_weight,
                       f.max_weight AS max_weight,
                       f.min_length AS min_length,
                       f.max_length AS max_length,
                       CASE WHEN f.name IS NULL THEN 1 ELSE 0 END AS is_trash
                FROM caught_fish cf
                LEFT JOIN fish f ON TRIM(cf.fish_name) = f.name
                LEFT JOIN trash t ON TRIM(cf.fish_name) = t.name
                WHERE cf.user_id = ?
                ORDER BY cf.weight DESC
            ''', (USER_ID,))
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            caught = [dict(zip(cols, r)) for r in rows]
            for item in caught:
                if item.get('is_trash'):
                    continue
                item['price'] = db.calculate_fish_price(item, item.get('weight', 0), item.get('length', 0))
    else:
        caught = db.get_caught_fish(USER_ID, CHAT_ID)
    print(f"Total caught rows returned: {len(caught)}")

    # Partition into sold / unsold
    sold = [f for f in caught if int(f.get('sold') or 0) == 1]
    unsold = [f for f in caught if int(f.get('sold') or 0) == 0]

    print(f"Sold items: {len(sold)}\nUnsold items: {len(unsold)}\n")

    # Compute expected XP from sold items using DB helper
    per_item = []
    total_expected_xp = 0
    for item in sold:
        # The DB helper expects keys like 'weight', 'min_weight', 'max_weight', 'rarity', 'is_trash'
        xp = db.calculate_item_xp(item)
        per_item.append({
            'id': item.get('id'),
            'name': item.get('fish_name'),
            'weight': item.get('weight'),
            'rarity': item.get('rarity'),
            'price': item.get('price'),
            'xp': xp,
        })
        total_expected_xp += xp

    # Sort by xp desc for visibility
    per_item.sort(key=lambda x: x['xp'], reverse=True)

    print("Per-sold-item XP (top 50):")
    pp.pprint(per_item[:50])
    print('\nTotal expected XP from sold items:', total_expected_xp)

    # Also compute expected XP from ALL items (in case some sold were not flagged)
    total_all_xp = sum(db.calculate_item_xp(it) for it in caught)
    print('Total expected XP from all caught items:', total_all_xp)

    # Compute expected levels from XP
    try:
        expected_level_from_sold = db.get_level_from_xp(total_expected_xp)
        sold_progress = db.get_level_progress(total_expected_xp)
        expected_level_from_all = db.get_level_from_xp(total_all_xp)
        all_progress = db.get_level_progress(total_all_xp)

        print(f"\nExpected level (from sold XP): {expected_level_from_sold} - XP into level: {sold_progress.get('xp_into_level')} / {sold_progress.get('xp_needed')}")
        print(f"Expected level (from all caught XP): {expected_level_from_all} - XP into level: {all_progress.get('xp_into_level')} / {all_progress.get('xp_needed')}")
    except Exception as e:
        print('Could not compute expected levels:', e)

    # Player stored XP
    player = db.get_player(USER_ID, CHAT_ID)
    if player:
        stored_xp = int(player.get('xp') or 0)
        stored_level = int(player.get('level') or 0)
        print(f"\nPlayer stored XP: {stored_xp} (level: {stored_level})")
        print(f"Difference (expected_from_sold - stored): {total_expected_xp - stored_xp}")
        try:
            # Compare stored level with expected from sold XP
            print(f"Level difference (expected_from_sold - stored_level): {expected_level_from_sold - stored_level}")
        except Exception:
            pass
    else:
        print('\nPlayer row not found with provided user_id/chat_id lookup.')

    # Helpful totals by weight / price
    total_sold_weight = sum(float(it.get('weight') or 0) for it in sold)
    total_sold_value = sum(int(it.get('price') or 0) for it in sold)
    print(f"\nSold weight total: {total_sold_weight:.3f} kg")
    print(f"Sold coins total: {total_sold_value} 🪙")

    print('\nDone.')
