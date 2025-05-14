import os.path
import re
import time
from googleapiclient.http import BatchHttpRequest
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


MAX_PER_BATCH = 100
MAX_MESSAGE_RESULTS = 500
USER_ID = "me"
BATCH_URI = "https://gmail.googleapis.com/batch/gmail/v1"
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def authenticate():
    """Handles user authentication and returns the credentials object."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def extract_email(from_field):
    """Extract email from from field."""
    match = re.search(r'<(.+?)>', from_field)

    if match:
        email = match.group(1)
        return email


def fetch_all_message_ids(service):
    """Get all message IDs from inbox."""
    all_ids = []
    next_page_token = None

    while True:
        response = (
            service.users()
            .messages()
            .list(
                userId=USER_ID,
                maxResults=MAX_MESSAGE_RESULTS,
                labelIds=["INBOX"],
                pageToken=next_page_token,
            )
            .execute()
        )
        messages = response.get("messages", [])
        all_ids.extend([m["id"] for m in messages])
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return all_ids


def fetch_from_headers_in_batches(service, message_ids):
    """Fetch From headers using Gmail batch API."""
    total = len(message_ids)
    sender_emails = set()

    def callback(request_id, response, exception):
        if exception is None:
            headers = response["payload"].get("headers", [])
            for header in headers:
                if header["name"].lower() == "from":
                    sender_emails.add(header["value"])


    for i in range(0, total, MAX_PER_BATCH):
        
        batch = BatchHttpRequest(
            callback=callback,
            batch_uri=BATCH_URI,
        )
        
        for msg_id in message_ids[i : i + MAX_PER_BATCH]:
            batch.add(
                service.users()
                .messages()
                .get(
                    userId=USER_ID,
                    id=msg_id,
                    format="metadata",
                    metadataHeaders=["From"],
                )
            )
        batch.execute()

        time.sleep(0.5)  # slight pause to respect rate limits
        print(
            f"✅ Processed batch {i // MAX_PER_BATCH + 1} / {((total - 1) // MAX_PER_BATCH) + 1}"
        )

    return sender_emails


def run():
    service = authenticate()
    print("🔄 Fetching all message IDs...")
    message_ids = fetch_all_message_ids(service)
    print(f"📩 Total messages found: {len(message_ids)}")

    print("🔍 Fetching 'From' headers in batches...")
    senders = fetch_from_headers_in_batches(service, message_ids)
    return senders
