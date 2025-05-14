from db.database import create_table, insert_record, fetch_records
from google_auth.auth import run


if __name__ == "__main__":
    create_table()
    existing_senders = fetch_records()
    current_senders = run()
    insert_record(current_senders)
    print(
        f"\n✅ Sucessfully inserted sender email addresses ({len(current_senders)} total):"
    )
