import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

def parse_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[datetime]:
    """
    Attempts to parse a date from common formats.
    - If your sheet uses ISO like 2024-01-15, this will work.
    - If your sheet uses other formats, add them below.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None

    # Common formats
    formats = [
        "%Y-%m-%d",          # 2024-01-15
        "%Y/%m/%d",          # 2024/01/15
        "%m/%d/%Y",          # 01/15/2024
        "%d/%m/%Y",          # 15/01/2024
        "%B %d, %Y",         # January 15, 2024
        "%b %d, %Y",         # Jan 15, 2024
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    # If it doesn't match, store it as None (or store raw string if preferred)
    return None

def get_sheet_records(service_account_file: str, sheet_id: str, worksheet_name: str) -> List[Dict[str, Any]]:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    # Uses row 1 as headers
    raw = ws.get_all_records()

    records: List[Dict[str, Any]] = []
    for r in raw:
        doc: Dict[str, Any] = {
            "title": str(r.get("title", "")).strip(),
            "authors": str(r.get("authors", "")).strip(),
            "year": parse_year(r.get("year")),
            "url": str(r.get("url", "")).strip(),
            "publisher": str(r.get("publisher", "")).strip(),
            "date": parse_date(r.get("date")),
            "image": str(r.get("image", "")).strip()
        }
        records.append(doc)

    return records

def upsert_to_mongodb(
    mongodb_uri: str,
    db_name: str,
    collection_name: str,
    records: List[Dict[str, Any]],
    unique_key: str = "url",
) -> Dict[str, int]:
    client = MongoClient(mongodb_uri)
    col = client[db_name][collection_name]

    if not records:
        return {"upserted": 0, "modified": 0, "matched": 0}

    now = datetime.now(timezone.utc)

    ops = []
    for doc in records:
        doc["_syncedAt"] = now

        key_val = doc.get(unique_key)
        if not key_val:
            # If a row has no URL, you can either skip it or handle differently
            continue

        ops.append(
            UpdateOne(
                {unique_key: key_val},
                {"$set": doc},
                upsert=True,
            )
        )

    if not ops:
        return {"upserted": 0, "modified": 0, "matched": 0}

    result = col.bulk_write(ops, ordered=False)

    return {
        "upserted": result.upserted_count,
        "modified": result.modified_count,
        "matched": result.matched_count,
    }
def main():
    load_dotenv()

    mongodb_uri = os.environ["MONGODB_URI"]
    db_name = os.environ["MONGODB_DB"]
    collection_name = os.environ["MONGODB_COLLECTION"]

    service_account_file = os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"]
    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    worksheet_name = os.environ.get("GOOGLE_WORKSHEET_NAME", "Sheet1")

    unique_key = os.environ.get("UNIQUE_KEY_COLUMN", "url")

    print("Reading Google Sheet...")
    records = get_sheet_records(service_account_file, sheet_id, worksheet_name)
    print(f"Fetched {len(records)} rows.")

    print(f"Syncing to MongoDB (upsert by '{unique_key}')...")
    stats = upsert_to_mongodb(mongodb_uri, db_name, collection_name, records, unique_key=unique_key)

    print("Done.")
    print(stats)


if __name__ == "__main__":
    main()