import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from typing import Union

def parse_year(value: Any) -> Union[int, str]:
    if value is None:
        return ""
    s = str(value).strip()
    if s == "":
        return ""
    try:
        return int(s)
    except ValueError:
        return ""

def parse_date(value: Any) -> Union[datetime, str]:
    """
    Attempts to parse a date from common formats.
    - If your sheet uses ISO like 2024-01-15, this will work.
    - If your sheet uses other formats, add them below.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if s == "":
        return ""

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
    return ""

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

            # Publications specific fields
            **({"unique_id": str(r.get("unique_id", "")).strip()} if worksheet_name == "Presentations" else {}),
            #Publications specific fields
            **({"year": parse_year(r.get("year"))} if worksheet_name == "Publications" else {}),

            #Publications and Preprints specific fields
            **({"authors": str(r.get("authors", "")).strip()} if worksheet_name in ["Publications", "Preprints"] else {}),
            **({"publisher": str(r.get("publisher", "")).strip()} if worksheet_name in ["Publications", "Preprints"] else {}),
            **({"doi": str(r.get("doi", "")).strip()} if worksheet_name in ["Publications", "Preprints"] else {}),

            #Common fields
            **({"url": str(r.get("url", "")).strip()} if worksheet_name in ["Publications", "Presentations"] else {}),
            **({"date": parse_date(r.get("date"))} if worksheet_name in ["Publications", "Presentations"] else {"date": parse_year(r.get("date"))}),

            #Presentations specific fields
            **({"event": str(r.get("event", "")).strip()} if worksheet_name == "Presentations" else {}),
            **({"location": str(r.get("location", "")).strip()} if worksheet_name == "Presentations" else {}),
            **({"format": str(r.get("format", "")).strip()} if worksheet_name == "Presentations" else {}),
            
            "image": str(r.get("image", "")).strip()
        }
        records.append(doc)

    return records

def upsert_to_mongodb(
    mongodb_uri: str,
    db_name: str,
    collection_name: str,
    records: List[Dict[str, Any]]
) -> Dict[str, int]:
    client = MongoClient(mongodb_uri)
    col = client[db_name][collection_name]

    if not records:
        return {"upserted": 0, "modified": 0, "matched": 0}

    now = datetime.now(timezone.utc)

    ops = []
    for doc in records:
        doc["_syncedAt"] = now

        if collection_name == "publications":
            filter_doc = {"url": doc.get("url", "")}
            if filter_doc["url"] == "":
                continue # Skip if no url
        elif collection_name == "presentations":
            filter_doc = {"unique_id": doc.get("unique_id", "")}
            if filter_doc["unique_id"] == "":
                continue # Skip if no unique_id
        else:
            filter_doc = {"doi": doc.get("doi", "")}
            if filter_doc["doi"] == "":
                continue # Skip if no doi

        ops.append(
            UpdateOne(
                filter_doc,
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
    collection_names = os.environ["MONGODB_COLLECTIONS"].split(",")

    service_account_file = os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"]
    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    worksheet_names = os.environ.get("GOOGLE_WORKSHEET_NAMES", "Sheet1").split(",")


    for worksheet_name, collection_name in zip(worksheet_names, collection_names):
        print(f"Reading {worksheet_name} from Google Sheets...")
        records = get_sheet_records(service_account_file, sheet_id, worksheet_name)
        print(f"Fetched {len(records)} rows.")

        print(f"Syncing to MongoDB (upsert into '{collection_name}')...")
        stats = upsert_to_mongodb(mongodb_uri, db_name, collection_name, records)

        print("Done.")
        print(f'{stats} to {collection_name} collection')


if __name__ == "__main__":
    main()