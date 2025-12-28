import mailbox
import sqlite3
import email.utils
from email.header import decode_header
from datetime import datetime, timezone
from pathlib import Path
from mailbox_path import _mailbox_path

DB_PATH = "mail.sqlite"

def decode_mime_header(value: str) -> str:
    """Decode RFC2047-encoded headers safely."""
    if not value:
        return ""
    parts = []
    for text, enc in decode_header(value):
        if isinstance(text, bytes):
            parts.append(text.decode(enc or "utf-8", errors="replace"))
        else:
            parts.append(text)
    return "".join(parts)

def parse_date_to_iso(date_str: str) -> str:
    """Convert Date header to ISO8601 (UTC). Returns '' if unknown."""
    if not date_str:
        return ""
    try:
        dt = email.utils.parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.isoformat(timespec="seconds")
    except Exception:
        return ""

def get_plain_text_preview(msg, limit=2000) -> str:
    """Return a short plain-text preview (skip attachments)."""
    try:
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    disp = (part.get("Content-Disposition") or "").lower()
                    if "attachment" in disp:
                        continue
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        return payload.decode(charset, errors="replace")[:limit]
            return ""
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")[:limit]
            return ""
    except Exception:
        return ""

def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        id INTEGER PRIMARY KEY,
        message_id TEXT,
        date_iso TEXT,          -- ISO8601 in UTC
        date_day TEXT,          -- YYYY-MM-DD (UTC)
        from_addr TEXT,
        to_addr TEXT,
        cc_addr TEXT,
        subject TEXT,
        body_preview TEXT
    )
    """)

    # Uniqueness helps avoid duplicates if the mbox is weird
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_message_id ON emails(message_id)")

    # Indexes for fast stats + searches
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date_day ON emails(date_day)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_from ON emails(from_addr)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_to ON emails(to_addr)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_subject ON emails(subject)")
    conn.commit()

def ingest(mbox_path: str, db_path: str = DB_PATH, store_preview: bool = True):
    conn = sqlite3.connect(db_path)
    init_db(conn)

    mbox = mailbox.mbox(mbox_path)

    batch = []
    BATCH_SIZE = 2000

    inserted = 0
    skipped = 0

    for i, msg in enumerate(mbox):
        message_id = msg.get("Message-Id", "") or ""

        # If there is no Message-Id, you *can* still insert (but may get dupes).
        # We'll allow it by not enforcing uniqueness if empty.
        date_iso = parse_date_to_iso(msg.get("Date", ""))
        date_day = date_iso[:10] if date_iso else ""

        from_addr = decode_mime_header(msg.get("From", ""))
        to_addr = decode_mime_header(msg.get("To", ""))
        cc_addr = decode_mime_header(msg.get("Cc", ""))
        subject = decode_mime_header(msg.get("Subject", ""))

        body_preview = get_plain_text_preview(msg) if store_preview else ""

        row = (message_id, date_iso, date_day, from_addr, to_addr, cc_addr, subject, body_preview)
        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            inserted_now, skipped_now = flush_batch(conn, batch)
            inserted += inserted_now
            skipped += skipped_now
            batch.clear()

            if (i + 1) % 10_000 == 0:
                print(f"Processed {i+1:,} messages | inserted {inserted:,} | skipped {skipped:,}")

    if batch:
        inserted_now, skipped_now = flush_batch(conn, batch)
        inserted += inserted_now
        skipped += skipped_now

    conn.close()
    print(f"Done. Inserted {inserted:,}, skipped {skipped:,}. DB: {db_path}")

def flush_batch(conn, batch):
    inserted = 0
    skipped = 0
    for row in batch:
        message_id = row[0]
        try:
            if message_id:
                conn.execute("""
                    INSERT OR IGNORE INTO emails
                    (message_id, date_iso, date_day, from_addr, to_addr, cc_addr, subject, body_preview)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
            else:
                # no message_id -> just insert (could duplicate)
                conn.execute("""
                    INSERT INTO emails
                    (message_id, date_iso, date_day, from_addr, to_addr, cc_addr, subject, body_preview)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
            inserted += 1
        except Exception:
            skipped += 1
    conn.commit()
    return inserted, skipped

if __name__ == "__main__":
    # Change this to your mbox path
    ingest(_mailbox_path()[0], db_path=DB_PATH, store_preview=True)
