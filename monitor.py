import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import requests

CONFIG_PATH = "config.json"
CSV_PATH = "warehouse_logs.csv"
STATE_PATH = "state.json"

@dataclass
class WindowState:
    window_start: datetime
    sum_qty: int

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def load_state():
    if not os.path.exists(STATE_PATH):
        return {"last_event_id": 0}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def parse_dt(dt_str: str, tz: ZoneInfo) -> datetime:
    # ожидаем формат: "YYYY-MM-DD HH:MM:SS"
    naive = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return naive.replace(tzinfo=tz)

def msk_day_key(dt: datetime) -> str:
    # ключ дня для сброса в 00:00 МСК
    return dt.strftime("%Y-%m-%d")

def build_role_mentions(role_ids):
    return " ".join(f"<@&{rid}>" for rid in role_ids)

def send_discord_embed(webhook_url: str, role_ids, payload: dict):
    # payload уже содержит готовые поля
    content = build_role_mentions(role_ids)

    data = {
        "content": content,
        "allowed_mentions": {
            "parse": [],
            "roles": role_ids
        },
        "embeds": [payload]
    }
    r = requests.post(webhook_url, json=data, timeout=10)
    r.raise_for_status()

def make_embed(player_name, static_id, item_name, item_id, qty_sum, limit, ts, warehouse_id):
    title = "⚠️ Возможный слив склада"
    desc = "Игрок взял слишком много предметов в течение одного часа."

    fields = [
        {"name": "Nickname", "value": f"`{player_name}`", "inline": True},
        {"name": "Static ID", "value": f"`{static_id}`", "inline": True},
        {"name": "Fraction", "value": f"`{warehouse_id}`", "inline": True},
        {"name": "Item", "value": f"`{item_name}` (ID `{item_id}`)", "inline": False},
        {"name": "Total taken", "value": f"`{qty_sum}` / limit `{limit}`", "inline": True},
        {"name": "Triggered at", "value": f"`{ts.strftime('%Y-%m-%d %H:%M:%S')}`", "inline": True},
    ]
    return {
        "title": title,
        "description": desc,
        "color": 15158332,  # красный
        "fields": fields,
        "timestamp": ts.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def should_trigger(sum_qty: int, limit: int, mode: str) -> bool:
    if mode == "gt":
        return sum_qty > limit
    return sum_qty >= limit  # gte by default

def read_new_rows(last_event_id: int):
    if not os.path.exists(CSV_PATH):
        return []

    rows = []
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                eid = int(row["event_id"])
            except Exception:
                continue
            if eid > last_event_id:
                rows.append(row)

    # сортируем на всякий случай
    rows.sort(key=lambda r: int(r["event_id"]))
    return rows

def main():
    cfg = load_config()
    tz = ZoneInfo(cfg.get("timezone", "Europe/Moscow"))
    webhook_url = cfg["discord_webhook_url"]
    role_ids = cfg.get("alert_roles", [])
    poll_sec = int(cfg.get("poll_interval_sec", 2))
    window_minutes = int(cfg.get("window_minutes", 60))
    threshold_mode = cfg.get("threshold_mode", "gte")

    items_cfg = cfg.get("items", {})
    # items_cfg keys are strings in json
    def get_item_cfg(item_id: int):
        return items_cfg.get(str(item_id))

    state = load_state()
    last_event_id = int(state.get("last_event_id", 0))

    # memory state:
    # windows[(day_key, player_static_id, item_id, warehouse_id)] = WindowState(...)
    windows = {}
    # alerted keys for "one per case per day"
    alerted = set()

    print(f"[monitor] start. last_event_id={last_event_id}")
    while True:
        try:
            rows = read_new_rows(last_event_id)
            for row in rows:
                eid = int(row["event_id"])
                player_name = row["player_name"]
                static_id = int(row["player_static_id"])
                action = row["action"].upper().strip()
                item_id = int(row["item_id"])
                qty = int(row["quantity"])
                ts = parse_dt(row["ts_msk"], tz)
                warehouse_id = row.get("warehouse_id", "N/A")

                last_event_id = eid

                if action != "TAKE":
                    continue

                item_cfg = get_item_cfg(item_id)
                if not item_cfg:
                    # предмет не отслеживаем
                    continue

                limit = int(item_cfg["max_per_window"])
                item_name = item_cfg.get("name", f"Item {item_id}")

                day = msk_day_key(ts)
                key = (day, static_id, item_id, warehouse_id)
                alert_key = (day, static_id, item_id, warehouse_id)

                # сброс окна если оно старое
                if key not in windows:
                    windows[key] = WindowState(window_start=ts, sum_qty=0)

                w = windows[key]
                if ts - w.window_start >= timedelta(minutes=window_minutes):
                    # новое окно
                    windows[key] = WindowState(window_start=ts, sum_qty=0)
                    w = windows[key]

                w.sum_qty += qty

                if alert_key not in alerted and should_trigger(w.sum_qty, limit, threshold_mode):
                    embed = make_embed(
                        player_name=player_name,
                        static_id=static_id,
                        item_name=item_name,
                        item_id=item_id,
                        qty_sum=w.sum_qty,
                        limit=limit,
                        ts=ts,
                        warehouse_id=warehouse_id
                    )
                    send_discord_embed(webhook_url, role_ids, embed)
                    alerted.add(alert_key)
                    print(f"[ALERT] {player_name} ({static_id}) item={item_id} sum={w.sum_qty} limit={limit} day={day}")

            # сохраняем прогресс
            state["last_event_id"] = last_event_id
            save_state(state)

        except Exception as e:
            print(f"[monitor] error: {e}")

        time.sleep(poll_sec)

if __name__ == "__main__":
    main()
