import json
import os
import time
from typing import Annotated, TypedDict, cast

from fastapi import FastAPI, HTTPException, Query

db_file = ".sdb"
db_format_version = 1


class Entry(TypedDict):
    value: str
    expires_at: float | None


Database = dict[str, Entry]


def default_db() -> Database:
    return {"default": {"value": "default", "expires_at": None}}


def load_db() -> Database:
    with open(db_file, "r", encoding="utf-8") as f:
        stored_db = json.load(f)

    if stored_db.get("version") == db_format_version and isinstance(
        stored_db.get("entries"), dict
    ):
        return cast(Database, stored_db["entries"])

    return {
        key: {"value": value, "expires_at": None} for key, value in stored_db.items()
    }


def save_db(db: Database) -> None:
    with open(db_file, "w", encoding="utf-8") as f:
        json.dump({"version": db_format_version, "entries": db}, f)


# Check if the database file exists, if not populate it
if not os.path.isfile(db_file):
    save_db(default_db())

app = FastAPI()


@app.put("/db")
async def put(
    key: str,
    value: str,
    ttl_seconds: Annotated[int | None, Query(gt=0)] = None,
):
    db = load_db()
    expires_at = None if ttl_seconds is None else time.time() + ttl_seconds
    db[key] = {"value": value, "expires_at": expires_at}
    save_db(db)
    return value


@app.get("/db")
async def get(key: str):
    try:
        db = load_db()
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(
            status_code=503,
            detail=f"Database file {db_file} could not be opened and loaded",
        ) from None

    entry = db.get(key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"No value set for key {key}")

    expires_at = entry["expires_at"]
    if expires_at is not None and time.time() >= expires_at:
        del db[key]
        save_db(db)
        raise HTTPException(status_code=404, detail=f"No value set for key {key}")

    return entry["value"]
