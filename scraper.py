#!/usr/bin/env python3
"""
Scraper for NEU WhatsApp housing listing chats.
Parses data/chat-*.txt and writes structured listing data to listings.csv.
"""

import re
import csv
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_FILE = "listings.csv"

# ── Filtering ──────────────────────────────────────────────────────────────────

SYSTEM_RE = re.compile(
    r"joined using a group link|joined from the community|was added|"
    r"removed .+|created this group|changed their phone number|"
    r"changed the group|turned (?:on|off) admin|"
    r"Messages and calls are end-to-end encrypted|"
    r"Disappearing messages were turned on|"
    r"image omitted|video omitted|audio omitted|sticker omitted|"
    r"This message was deleted|Waiting for this message|document omitted|"
    r"You joined|GIF omitted|contact card omitted|deleted the group description|"
    r"changed this group's settings",
    re.IGNORECASE,
)

LISTING_RE = re.compile(
    r"accommodation|private room|shared room|hall spot|sublet|"
    r"\$\s*\d+\s*/\s*month|\$\d+.*?month|"
    r"rent\s*[:\-=]?\s*\$|lease|move.?in|"
    r"bed.*?bath|\d\s*bhk|"
    r"looking for.{0,40}(?:room|accommodation|spot)|"
    r"(?:room|spot|accommodation)\s+available|housing\s+available",
    re.IGNORECASE,
)

EXCLUDE_RE = re.compile(
    r"move\s*out\s*sale|moveout\s*sale|"
    r"car\s*rental|"
    r"(?:bed\s*frame|mattress|office\s*chair|dumbbell|yoga\s*mat|lamp|sofa|mirror)\s*[-–]\s*\$",
    re.IGNORECASE,
)

# ── Chat parsing ───────────────────────────────────────────────────────────────

MSG_RE = re.compile(
    r"^\[(\d{1,2}/\d{1,2}/\d{2,4},\s*\d{1,2}:\d{2}:\d{2}\s*(?:AM|PM)?)\]\s+"
    r"([^:\u200e\u200f\ufeff\u202a\u202c]+?):\s*(.*)"
)


def parse_chat(filepath: Path) -> list[dict]:
    messages = []
    current = None
    with open(filepath, encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n").lstrip("\ufeff\u200e\u200f\u202a\u202c")
            m = MSG_RE.match(line)
            if m:
                if current:
                    messages.append(current)
                ts, sender, text = m.groups()
                current = {
                    "timestamp": ts.strip(),
                    "sender": sender.strip().lstrip("~ "),
                    "text": text,
                    "source": filepath.name,
                }
            elif current:
                current["text"] += "\n" + line
    if current:
        messages.append(current)
    return messages


def is_listing(text: str) -> bool:
    if SYSTEM_RE.search(text):
        return False
    if EXCLUDE_RE.search(text):
        return False
    if not LISTING_RE.search(text):
        return False
    # Require enough substance to be a real listing
    if len(text.strip()) < 60:
        return False
    return True


# ── Field extractors ───────────────────────────────────────────────────────────


def extract_listing_type(text: str) -> str | None:
    t = text.lower()
    # Offering: poster has a spot and wants people to fill it
    if re.search(r"looking for \d+ (?:people|roommates|tenants)", t):
        return "offering"
    if re.search(
        r"\b(?:accommodation|room|spot|housing|sublet)\s+available\b|"
        r"\bavailable\s+for\s+(?:\d|a\b|one\b)|"
        r"permanent\s+accommodation|housing\s+available|"
        r"spots?\s+available|"
        r"\bavailable\s+from\b|"                          # "Available from May 3rd"
        r"\bfor\s+rent\b|"                                # "Private Room for Rent"
        r"\bsubletting\b|\bsublet\b|"                     # any sublet mention = offering
        r"sharing\s+room|shared\s+accommodation\s+for|"  # "Sharing Room 2 spots"
        r"\baccommodation\b.{0,60}?\bfor\s+(?:a\s+)?(?:one|male|female|boy|girl)\b|"
        r"private\s+room\b[\s\S]{0,80}?\$|"               # "Private Room ... $X" (multiline)
        r"shared\s+(?:hall\s+)?spot|"                     # "Shared Hall Spot" (no \b, markdown-safe)
        r"off.lease\s+private\s+room|"                    # "Off-Lease Private Room Deal"
        r"spacious\s+bedroom\s+available|"                # "Huge Spacious Bedrooms AVAILABLE"
        r"\$\s*\d+\s*/\s*mo\b",                           # "$550/mo" alone implies offering
        t,
    ):
        return "offering"
    # Seeking: poster needs a place
    if re.search(
        r"(?:i'?m?\s+)?looking for\b.{0,40}?"
        r"(?:room|accommodation|spot|place|housing|private|shared)",
        t,
    ):
        return "seeking"
    if re.search(r"\bneed(?:ing)?\s+(?:a\s+)?(?:room|accommodation|place|housing)\b", t):
        return "seeking"
    if re.search(r"\bsearching for\b|\bin search of\b|\brequire.*?accommodation\b", t):
        return "seeking"
    return None


def extract_accommodation_type(text: str) -> str | None:
    t = text.lower()
    if "permanent" in t:
        return "permanent"
    if re.search(r"\btemporary\b|\bsublet\b|\bshort.term\b", t):
        return "temporary"
    return None


def extract_room_type(text: str) -> str | None:
    t = text.lower()
    if re.search(r"hall\s+spot", t):
        return "hall_spot"
    if re.search(r"private\s+room", t):
        return "private_room"
    if re.search(r"shared\s+room|shared\s+spot", t):
        return "shared_room"
    return None


def extract_on_lease(text: str) -> str | None:
    t = text.lower()
    if re.search(r"off.?lease", t):
        return "off_lease"
    if re.search(r"on.?lease", t):
        return "on_lease"
    return None


_STREET_SUFFIX = (
    r"(?:st(?:reet)?|ave(?:nue)?|r(?:oa)?d|blvd|boulevard|"
    r"dr(?:ive)?|ln|lane|way|ct|court|pl(?:ace)?|hwy|highway|"
    r"ter(?:race)?|pkwy|parkway|cir(?:cle)?)"
)

_ADDRESS_RE = re.compile(
    # Street number — digit(s) + optional letter (e.g. 15B), MUST be followed by a space
    # so ordinals like "1st", "31st" are never matched.
    r"\d+[A-Za-z]?\s+"
    # Street name: 1–4 words made of letters/hyphens/apostrophes only (no digits,
    # no commas), each followed by a space.  This prevents sentences like
    # "1 shared room spot available … St" from matching.
    r"(?:[A-Za-z][A-Za-z\'\-\.]*\s+){1,4}"
    # Street suffix word boundary
    + _STREET_SUFFIX + r"\b"
    # Optional unit / apt designation
    r"(?:[,.]?\s*(?:unit|apt|apartment|suite|#)\.?\s*[\w\-]+)?"
    # Optional city (up to 3 words) + optional state/zip
    r"(?:,\s*[A-Za-z][A-Za-z\s]{0,25}(?:,\s*(?:MA|Massachusetts)\s*\d{0,5})?)?",
    re.IGNORECASE,
)


def extract_address(text: str) -> str | None:
    m = _ADDRESS_RE.search(text)
    return m.group().strip().rstrip(",.") if m else None


NEIGHBORHOODS = [
    "fenway", "roxbury", "brighton", "longwood", "jamaica plain",
    "brookline", "malden", "back bay", "south end", "mission hill",
    "allston", "somerville", "cambridge", "fort hill", "hyde park",
    "west roxbury", "dorchester", "mattapan", "roslindale",
    "medford", "quincy", "newton",
]


def extract_neighborhood(text: str) -> str | None:
    t = text.lower()
    found = [n for n in NEIGHBORHOODS if n in t]
    return ", ".join(found) if found else None


def extract_available_from(text: str) -> str | None:
    for p in [
        r"(?:available\s*(?:from)?|move.?in)\s*:?\s*"
        r"((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\.?\s+"
        r"\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})",

        r"(?:available\s*(?:from)?|move.?in)\s*:?\s*"
        r"(\d{1,2}(?:st|nd|rd|th)?\s+"
        r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\.?,?\s*\d{4})",

        r"starting\s+(?:from\s+)?"
        r"((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\.?\s+"
        r"\d{1,2}(?:st|nd|rd|th)?(?:,?\s*\d{4})?)",

        r"available\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def extract_lease_end(text: str) -> str | None:
    for p in [
        r"(?:lease|until|through|–|-|to)\s+"
        r"(aug(?:ust)?\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})",

        r"(?:ends?|end)\s+(?:in|on)?\s+"
        r"((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\.?\s+\d{4})",

        r"lease\s+(?:ends?|till|end\s+date)\s*:?\s*"
        r"((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\.?\s+\d{4})",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def extract_monthly_rent(text: str) -> str | None:
    for p in [
        r"\$\s*([\d,]+)\s*/\s*month",
        r"\$\s*([\d,]+)\s*per\s*month",
        r"rent\s*[:\-=]?\s*\$\s*([\d,]+)",
        r"\$\s*([\d,]+)\s*/mo\b",
        r"([\d,]+)\s*\$\s*/\s*month",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1).replace(",", "")
    return None


def extract_bedrooms(text: str) -> str | None:
    m = re.search(r"(\d+)\s*(?:bed(?:room)?s?|BHK|BR)\b", text, re.IGNORECASE)
    return m.group(1) if m else None


def extract_bathrooms(text: str) -> str | None:
    m = re.search(r"(\d+(?:[./]\d+)?)\s*bath(?:room)?s?\b", text, re.IGNORECASE)
    return m.group(1) if m else None


def extract_total_occupants(text: str) -> str | None:
    for p in [
        r"total\s+(\d+)\s+people",
        r"(\d+)\s+people\s+(?:total|living\s+in)",
        r"shared\s+by\s+(?:only\s+)?(\d+)\s+people",
        r"(\d+)\s+(?:people|roommates)\s+in\s+the\s+apt",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def extract_sqft(text: str) -> str | None:
    m = re.search(r"([\d,]+)\s*sq\.?\s*ft", text, re.IGNORECASE)
    return m.group(1).replace(",", "") if m else None


def extract_utilities(text: str) -> dict:
    t = text.lower()
    return {
        "heat_included":        bool(re.search(r"heat\s+(?:and\s+(?:hot\s+)?water\s+)?incl(?:uded)?", t)),
        "water_included":       bool(re.search(r"(?:hot\s+)?water\s+incl(?:uded)?", t)),
        "gas_included":         bool(re.search(r"\bgas\s+incl(?:uded)?", t)),
        "electricity_included": bool(re.search(r"electri(?:city)?\s+incl(?:uded)?", t)),
        "wifi_included":        bool(re.search(r"wi.?fi\s+incl(?:uded)?", t)),
    }


def extract_laundry(text: str) -> str | None:
    t = text.lower()
    if re.search(r"in.unit\s+(?:washer|laundry)|laundry\s+in.unit|in\s+unit\s+laundry", t):
        return "in_unit"
    if re.search(
        r"laundry\s+in\s+(?:the\s+)?(?:basement|building)|"
        r"in.house\s+laundry|in\s+building\s+laundry",
        t,
    ):
        return "in_building"
    if "laundry" in t:
        return "available"
    return None


def extract_furnished(text: str) -> str | None:
    t = text.lower()
    if "fully furnished" in t:
        return "fully_furnished"
    if "furnished" in t and "unfurnished" not in t:
        return "furnished"
    if "unfurnished" in t:
        return "unfurnished"
    return None


def extract_kitchen(text: str) -> bool | None:
    return True if re.search(r"fully\s+equipped\s+kitchen|equipped\s+kitchen", text, re.IGNORECASE) else None


def extract_parking(text: str) -> bool | None:
    return True if re.search(r"parking\s+(?:available|included)", text, re.IGNORECASE) else None


def extract_gender_preference(text: str) -> str | None:
    t = text.lower()
    if re.search(
        r"mixed.gender|any\s+gender|all\s+genders?|open\s+to\s+all\s+genders?|"
        r"no\s+gender\s+pref|boy\s+or\s+girl|girl\s+or\s+boy",
        t,
    ):
        return "any"
    if re.search(
        r"(?:for\s+(?:a\s+)?(?:one\s+)?|available\s+for\s+(?:a\s+)?(?:one\s+)?)(?:male|boy|man)\b|"
        r"all.(?:male|boys?)\s+apartment|male\s+replacement|boys?\s+apartment",
        t,
    ):
        return "male"
    if re.search(
        r"(?:for\s+(?:a\s+)?(?:one\s+)?|available\s+for\s+(?:a\s+)?(?:one\s+)?)(?:female|girl|woman)\b|"
        r"all.(?:female|girls?)\s+apartment|girls?\s+only|female\s+only|all.girls?",
        t,
    ):
        return "female"
    return None


def extract_food_preference(text: str) -> str | None:
    t = text.lower()
    if re.search(r"pure\s+veg(?:etarian)?|jain\b|vegan\b", t):
        return "strict_vegetarian"
    if re.search(r"\bvegetarian\b", t):
        if re.search(r"non.vegetarian|nonveg", t):
            return "no_restriction"
        return "vegetarian"
    if re.search(r"no\s+food\s+(?:pref|restrict)|any\s+food|no\s+food\s+restrictions?", t):
        return "no_restriction"
    if re.search(r"non.vegetarian|non\s+veg\b|nonveg\b", t):
        return "non_vegetarian"
    return None


def extract_smoking_allowed(text: str) -> bool | None:
    if re.search(r"no\s+smoking|non.smoking|smoking\s+not\s+allowed", text, re.IGNORECASE):
        return False
    if re.search(r"smoking\s+(?:allowed|ok|permitted)", text, re.IGNORECASE):
        return True
    return None


def extract_drinking_allowed(text: str) -> bool | None:
    if re.search(r"no\s+drink(?:ing)?|no\s+alcohol|strictly\s+no\s+drink", text, re.IGNORECASE):
        return False
    if re.search(r"drinking\s+(?:allowed|ok)", text, re.IGNORECASE):
        return True
    return None


def extract_walk_time_to_neu(text: str) -> str | None:
    for p in [
        r"(\d+)\s*min(?:ute)?s?\s+walk\s+to\s+(?:northeastern|NEU)\b",
        r"(\d+)\s*min(?:ute)?s?\s+from\s+(?:northeastern\s+university|NEU)\b",
        r"(\d+)\s*min(?:ute)?s?\s+to\s+northeastern\s+university",
        r"(\d+)\s*min\b.*?northeastern",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def extract_transit(text: str) -> str | None:
    parts = []
    for pattern, name in [
        (r"green\s+line", "Green Line"),
        (r"orange\s+line", "Orange Line"),
        (r"red\s+line", "Red Line"),
        (r"blue\s+line", "Blue Line"),
    ]:
        if re.search(pattern, text, re.IGNORECASE):
            parts.append(name)
    buses = re.findall(r"\bbus(?:es)?\s+(\d+(?:[,/&\s]+\d+)*)", text, re.IGNORECASE)
    if buses:
        nums = re.findall(r"\d+", " ".join(buses))
        parts.append("Bus " + "/".join(sorted(set(nums))))
    return "; ".join(parts) if parts else None


def extract_red_eye(text: str) -> bool | None:
    return True if re.search(r"red.?eye", text, re.IGNORECASE) else None


def extract_broker_fee(text: str) -> str | None:
    t = text.lower()
    if re.search(r"no\s+broker\s+fee|no\s+brokerage|zero\s+broker", t):
        return "none"
    m = re.search(r"broker(?:age)?\s*(?:fee)?\s*[:\-=]?\s*\$\s*([\d,]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).replace(",", "")
    if "broker" in t:
        return "yes"
    return None


def extract_security_deposit(text: str) -> str | None:
    for p in [
        r"security\s+deposit\s*[:\-=]?\s*\$\s*([\d,]+)",
        r"\$\s*([\d,]+)\s*(?:—\s*)?(?:security\s*)?deposit",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1).replace(",", "")
    if re.search(r"deposit", text, re.IGNORECASE):
        return "yes"
    return None


def extract_contact(text: str) -> str | None:
    phones = re.findall(
        r"(?:\+?1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}"
        r"|\+\d{1,3}[\s\-]?\d{4,6}[\s\-]?\d{4,6}"
        r"|\+\d{2,3}\s*\d{5}\s*\d{5}",
        text,
    )
    seen = dict.fromkeys(p.strip() for p in phones)
    return "; ".join(seen) if seen else None


def extract_pets(text: str) -> str | None:
    m = re.search(r"\b(cat|dog|pet|kitten)\b", text, re.IGNORECASE)
    return m.group(1).lower() if m else None


# ── Assembly ───────────────────────────────────────────────────────────────────

FIELDNAMES = [
    "post_date", "poster_name", "source_group",
    "listing_type", "accommodation_type", "room_type", "on_lease",
    "address", "neighborhood",
    "available_from", "lease_end_date",
    "monthly_rent", "broker_fee", "security_deposit",
    "bedrooms", "bathrooms", "total_occupants", "sqft",
    "heat_included", "water_included", "gas_included",
    "electricity_included", "wifi_included",
    "laundry", "kitchen_equipped", "furnished", "parking", "pets",
    "gender_preference", "food_preference",
    "smoking_allowed", "drinking_allowed",
    "walk_time_to_neu_min", "transit_lines", "red_eye_accessible",
    "contact_info",
    "raw_message",
]


def extract_fields(msg: dict) -> dict:
    text = msg["text"]
    u = extract_utilities(text)
    return {
        "post_date":            msg["timestamp"],
        "poster_name":          msg["sender"],
        "source_group":         msg["source"],
        "listing_type":         extract_listing_type(text),
        "accommodation_type":   extract_accommodation_type(text),
        "room_type":            extract_room_type(text),
        "on_lease":             extract_on_lease(text),
        "address":              extract_address(text),
        "neighborhood":         extract_neighborhood(text),
        "available_from":       extract_available_from(text),
        "lease_end_date":       extract_lease_end(text),
        "monthly_rent":         extract_monthly_rent(text),
        "broker_fee":           extract_broker_fee(text),
        "security_deposit":     extract_security_deposit(text),
        "bedrooms":             extract_bedrooms(text),
        "bathrooms":            extract_bathrooms(text),
        "total_occupants":      extract_total_occupants(text),
        "sqft":                 extract_sqft(text),
        "heat_included":        u["heat_included"] or None,
        "water_included":       u["water_included"] or None,
        "gas_included":         u["gas_included"] or None,
        "electricity_included": u["electricity_included"] or None,
        "wifi_included":        u["wifi_included"] or None,
        "laundry":              extract_laundry(text),
        "kitchen_equipped":     extract_kitchen(text),
        "furnished":            extract_furnished(text),
        "parking":              extract_parking(text),
        "pets":                 extract_pets(text),
        "gender_preference":    extract_gender_preference(text),
        "food_preference":      extract_food_preference(text),
        "smoking_allowed":      extract_smoking_allowed(text),
        "drinking_allowed":     extract_drinking_allowed(text),
        "walk_time_to_neu_min": extract_walk_time_to_neu(text),
        "transit_lines":        extract_transit(text),
        "red_eye_accessible":   extract_red_eye(text),
        "contact_info":         extract_contact(text),
        "raw_message":          text,
    }


def parse_post_date(ts: str):
    """Parse WhatsApp timestamp for sorting. Returns a comparable string (ISO-ish)."""
    for fmt in ("%m/%d/%y, %I:%M:%S %p", "%m/%d/%Y, %I:%M:%S %p",
                "%d/%m/%y, %I:%M:%S %p", "%d/%m/%Y, %I:%M:%S %p"):
        try:
            from datetime import datetime
            return datetime.strptime(ts.strip(), fmt)
        except ValueError:
            pass
    return None


def deduplicate(rows: list[dict]) -> list[dict]:
    """Keep the latest listing per address. Rows without an address are kept as-is."""
    # Sort all rows by parsed date descending so latest comes first
    rows.sort(key=lambda r: parse_post_date(r["post_date"]) or __import__("datetime").datetime.min, reverse=True)

    seen_addresses: set[str] = set()
    deduped = []
    no_address = []

    for row in rows:
        addr = (row.get("address") or "").strip().lower()
        if not addr:
            no_address.append(row)
            continue
        if addr not in seen_addresses:
            seen_addresses.add(addr)
            deduped.append(row)

    return deduped + no_address


def main():
    all_rows = []
    for chat_file in sorted(DATA_DIR.glob("chat-*.txt")):
        messages = parse_chat(chat_file)
        listings = [m for m in messages if is_listing(m["text"])]
        print(f"{chat_file.name}: {len(listings)} listings from {len(messages)} messages")
        all_rows.extend(extract_fields(m) for m in listings)

    print(f"Before dedup: {len(all_rows)}")
    all_rows = deduplicate(all_rows)
    print(f"After dedup:  {len(all_rows)}")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nTotal: {len(all_rows)} listings -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
