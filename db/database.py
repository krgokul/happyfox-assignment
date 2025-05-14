import sqlite3

DB_NAME = "emails.db"


def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE
        )
    """
    )

    conn.commit()
    conn.close()


def insert_record(email_list):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    email_data = [(email,) for email in email_list]
    cursor.executemany(
        """
        INSERT OR IGNORE INTO emails (email)
        VALUES (?)
    """,
        email_data,
    )

    conn.commit()
    conn.close()


def fetch_records():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT email FROM emails")
    email_list = cursor.fetchall()
    conn.close()

    return [email[0] for email in email_list]