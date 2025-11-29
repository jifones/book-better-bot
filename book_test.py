# book_test.py
import os
from dotenv import load_dotenv
from book_better.better.live_client import LiveBetterClient

load_dotenv()

client = LiveBetterClient(
    username=os.environ["BETTER_USERNAME_JAVIER"],
    password=os.environ["BETTER_PASSWORD_JAVIER"]
)

raw = client.get_raw_slots_for_day(
    venue_slug="islington-tennis-centre",
    activity_slug="highbury-tennis",
    target_date="2025-12-03",
)

print(">>> RAW SLOTS encontrados:", len(raw))
print(">>> Primer raw slot:")
print(raw[0])
