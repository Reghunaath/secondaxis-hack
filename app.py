import csv
import json
import math
import os
from datetime import datetime
from pathlib import Path
import requests as http
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

load_dotenv()

app = Flask(__name__)
CSV_FILE = Path("listings.csv")

JSONBIN_KEY = os.environ.get("JSONBIN_KEY")
JSONBIN_ID  = os.environ.get("JSONBIN_ID")
_HEADERS = lambda: {"X-Master-Key": JSONBIN_KEY, "Content-Type": "application/json"}


def _fb_read():
    r = http.get(f"https://api.jsonbin.io/v3/b/{JSONBIN_ID}/latest", headers=_HEADERS())
    return r.json().get("record", [])


def _fb_write(entries):
    http.put(f"https://api.jsonbin.io/v3/b/{JSONBIN_ID}", headers=_HEADERS(), json=entries)


def load_listings():
    rows = []
    with open(CSV_FILE, encoding="utf-8-sig") as f:
        for i, row in enumerate(csv.DictReader(f)):
            rent = row.get("monthly_rent", "")
            beds = row.get("bedrooms", "")
            baths = row.get("bathrooms", "")
            address = (row.get("address") or "").strip()
            neighborhood = (row.get("neighborhood") or "").split(",")[0].strip().title()
            acc_type = (row.get("accommodation_type") or "").strip().lower()
            room_type = (row.get("room_type") or "").strip()
            available_from = (row.get("available_from") or "").strip()
            lease_end = (row.get("lease_end_date") or "").strip()
            contact = (row.get("contact_info") or "").strip()
            msg = (row.get("raw_message") or "").strip()

            # Build a human title
            room_label = {
                "private_room": "Private Room",
                "shared_room": "Shared Room",
                "hall_spot": "Hall Spot",
            }.get(room_type, "Room")
            short_addr = address.split(",")[0] if address else "Boston"
            title = f"{room_label} at {short_addr}" if address else room_label

            # Duration string
            if available_from and lease_end:
                duration = f"{available_from} – {lease_end}"
            elif available_from:
                duration = f"From {available_from}"
            elif lease_end:
                duration = f"Until {lease_end}"
            else:
                duration = "See description"

            # Type normalised
            if acc_type == "permanent":
                listing_type = "Permanent"
            elif acc_type == "temporary":
                listing_type = "Temporary"
            else:
                listing_type = "Permanent"   # default

            # Extract first phone for WhatsApp link
            first_phone = ""
            if contact:
                import re
                nums = re.findall(r"[\d\+\-\(\)\s]{7,}", contact)
                if nums:
                    first_phone = re.sub(r"[^\d\+]", "", nums[0])

            rows.append({
                "id": i,
                "type": listing_type,
                "room_type": room_type,
                "title": title,
                "rent": int(float(rent)) if rent else None,
                "neighborhood": neighborhood or "Boston",
                "available_from": available_from,
                "lease_end": lease_end,
                "duration": duration,
                "beds": beds or "?",
                "baths": baths or "?",
                "address": address,
                "desc": msg[:300].replace("\n", " "),
                "full_desc": msg,
                "whatsapp": first_phone,
                "contact_raw": contact,
                "posted": (row.get("post_date") or "").strip(),
                "gender": (row.get("gender_preference") or "").strip(),
                "food": (row.get("food_preference") or "").strip(),
                "laundry": (row.get("laundry") or "").strip(),
                "kitchen": row.get("kitchen_equipped") or "",
                "furnished": (row.get("furnished") or "").strip(),
                "walk_neu": (row.get("walk_time_to_neu_min") or "").strip(),
                "transit": (row.get("transit_lines") or "").strip(),
                "red_eye": row.get("red_eye_accessible") or "",
                "on_lease": (row.get("on_lease") or "").strip(),
                "broker_fee": (row.get("broker_fee") or "").strip(),
                "heat": row.get("heat_included") or "",
                "water": row.get("water_included") or "",
                "smoking": row.get("smoking_allowed") or "",
                "parking": row.get("parking") or "",
                "sqft": (row.get("sqft") or "").strip(),
            })
    return rows


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/feedback")
def feedback():
    return render_template("feedback.html")


@app.route("/api/listings")
def api_listings():
    listings = load_listings()

    # -- filters from query params --
    q          = request.args.get("q", "").lower()
    ltype      = request.args.get("type", "")          # Permanent | Temporary
    nbhd       = request.args.getlist("neighborhood")  # can be multiple
    price_min  = request.args.get("price_min", type=int, default=0)
    price_max  = request.args.get("price_max", type=int, default=99999)
    beds       = request.args.get("beds", "")          # 1|2|3|4+
    move_month = request.args.get("move_month", "")
    sort       = request.args.get("sort", "newest")

    def matches(l):
        if ltype and l["type"] != ltype:
            return False
        if nbhd and l["neighborhood"] not in nbhd:
            return False
        rent = l["rent"]
        if rent is not None:
            if rent < price_min or rent > price_max:
                return False
        if beds:
            b = str(l["beds"])
            if beds == "4+":
                try:
                    if int(float(b)) < 4:
                        return False
                except ValueError:
                    return False
            else:
                try:
                    if int(float(b)) != int(beds):
                        return False
                except ValueError:
                    return False
        if move_month:
            if move_month.lower() not in (l["available_from"] or "").lower():
                return False
        if q:
            haystack = (l["title"] + " " + l["desc"] + " " + l["neighborhood"] + " " + l["address"]).lower()
            if q not in haystack:
                return False
        return True

    filtered = [l for l in listings if matches(l)]

    if sort == "price-asc":
        filtered.sort(key=lambda l: l["rent"] if l["rent"] is not None else 99999)
    elif sort == "price-desc":
        filtered.sort(key=lambda l: l["rent"] if l["rent"] is not None else 0, reverse=True)
    # else newest — already in file order (deduplicated by latest)

    # pagination
    page      = request.args.get("page", type=int, default=1)
    page_size = request.args.get("page_size", type=int, default=30)
    total     = len(filtered)
    start     = (page - 1) * page_size
    paginated = filtered[start: start + page_size]

    # counts for chips
    all_listings = load_listings()
    type_counts = {
        "all": len(all_listings),
        "Temporary": sum(1 for l in all_listings if l["type"] == "Temporary"),
        "Permanent":  sum(1 for l in all_listings if l["type"] == "Permanent"),
    }

    # unique neighborhoods for sidebar
    neighborhoods = sorted(set(
        l["neighborhood"] for l in all_listings if l["neighborhood"] and l["neighborhood"] != "Boston"
    ))

    return jsonify({
        "listings": paginated,
        "total": total,
        "page": page,
        "pages": math.ceil(total / page_size),
        "type_counts": type_counts,
        "neighborhoods": neighborhoods,
    })


@app.route("/api/feedback", methods=["GET"])
def get_feedback():
    return jsonify(_fb_read())


@app.route("/api/feedback", methods=["POST"])
def post_feedback():
    data = request.get_json(force=True)
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message required"}), 400
    entries = _fb_read()
    entries.insert(0, {
        "name": (data.get("name") or "Anonymous").strip() or "Anonymous",
        "type": data.get("type") or "general",
        "message": message,
        "date": datetime.now().strftime("%b %d, %Y"),
    })
    _fb_write(entries)
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
