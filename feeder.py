import csv
import os
import random
from datetime import datetime
from zoneinfo import ZoneInfo

CSV_PATH = "warehouse_logs.csv"
TZ = ZoneInfo("Europe/Moscow")

FIELDS = ["event_id", "player_name", "player_static_id", "action", "item_id", "quantity", "ts_msk", "warehouse_id"]

def ensure_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()

def get_last_event_id():
    if not os.path.exists(CSV_PATH):
        return 0
    last = 0
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                last = max(last, int(row["event_id"]))
            except:
                pass
    return last

def append_event(player_name, static_id, action, item_id, qty, warehouse_id="N/A"):
    ensure_csv()
    eid = get_last_event_id() + 1
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "event_id": eid,
        "player_name": player_name,
        "player_static_id": static_id,
        "action": action,
        "item_id": item_id,
        "quantity": qty,
        "ts_msk": ts,
        "warehouse_id": warehouse_id
    }
    with open(CSV_PATH, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writerow(row)
    print(f"[feeder] appended: {row}")

def interactive():
    print("Interactive mode. Example: Nick 12345 TAKE 1001 10 LSPD")
    while True:
        s = input("> ").strip()
        if not s:
            continue
        if s.lower() in ("exit", "quit"):
            break
        parts = s.split()
        if len(parts) < 5:
            print("Need: player_name static_id action item_id quantity [warehouse_id]")
            continue
        player_name = parts[0]
        static_id = int(parts[1])
        action = parts[2].upper()
        item_id = int(parts[3])
        qty = int(parts[4])
        warehouse_id = parts[5] if len(parts) >= 6 else "N/A"
        append_event(player_name, static_id, action, item_id, qty, warehouse_id)

def random_mode():
    players = [("Nick", 12345), ("Ivan", 22222), ("Max", 33333)]
    items = [1, 2, 3, 4]
    warehouses = ["Marabunta Grande"]
    while True:
        p = random.choice(players)
        action = random.choice(["TAKE"])
        item_id = random.choice(items)
        qty = random.randint(1, 50)
        wh = random.choice(warehouses)
        append_event(p[0], p[1], action, item_id, qty, wh)
        input("Enter to add next (or Ctrl+C)...")

if __name__ == "__main__":
    ensure_csv()
    mode = input("Mode (i=interactive, r=random): ").strip().lower()
    if mode == "r":
        random_mode()
    else:
        interactive()
