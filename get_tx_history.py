import requests
import time
import csv
import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_NAME = "pft_tracker"
DB_USER = "milk"
DB_PASSWORD = os.getenv("db_pw")  # leave blank if you're using peer auth
DB_HOST = "localhost"


def connect_db():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    return conn


def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pft_transfers (
                tx_hash TEXT PRIMARY KEY,
                day INTEGER,
                ledger_index INTEGER,
                sender TEXT,
                receiver TEXT,
                amount REAL,
                timestamp TEXT
            );
        """
        )
        conn.commit()


# Constants
XRPL_NODE_URL = "http://s1.ripple.com:51234/"
ISSUER_ADDRESS = "rnQUEEg8yyjrwk9FhyXpKavHyCRJM9BDMW"
PFT_CURRENCY_CODE = "PFT"

LEDGERS_PER_DAY = 22700
START_LEDGER = 87570565
BALANCES_CSV = "pft_balances_full.csv"
TRANSFERS_CSV = "pft_daily_transfers_full.csv"
DAILY_ACTIVE_CSV = "pft_daily_active.csv"
CIRCULATING_SUPPLY_CSV = "pft_circulating_supply.csv"
CHECKPOINT_FILE = "checkpoint.txt"


# Initial watchlist (starts with issuer)
watchlist = set([ISSUER_ADDRESS])


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return int(f.read().strip())
    return 0  # Default to day 0


def save_checkpoint(day_number):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(day_number))


# Helper: get ledger range for a given day
def get_ledger_range_for_day(day_number):
    ledger_min = START_LEDGER + (day_number * LEDGERS_PER_DAY)
    ledger_max = ledger_min + LEDGERS_PER_DAY
    return ledger_min, ledger_max


# Initialize counters
api_call_count = 0
log_file_index = 0
RESPONSES_LOG_TEMPLATE = "api_responses_log_{}.txt"


def safe_post(payload):
    global api_call_count, log_file_index

    response = requests.post(
        XRPL_NODE_URL, headers={"Content-Type": "application/json"}, json=payload
    )

    if response.status_code == 200:
        data = response.json()

        # Check for server warnings
        # if "warnings" in data:
        #     for warning in data["warnings"]:
        # print(f"Server warning: {warning['message']}")

        # Log the full response
        logfile_name = RESPONSES_LOG_TEMPLATE.format(log_file_index)
        with open(logfile_name, mode="a", encoding="utf-8") as logfile:
            logfile.write(json.dumps(data))
            logfile.write("\n")  # newline between entries

        api_call_count += 1

        # Rotate log every 1000 API calls
        if api_call_count >= 1000:
            api_call_count = 0
            log_file_index += 1

        return data

    else:
        print(f"Request failed: {response.status_code} - {response.text}")
        return None


# Helper: fetch account_tx for an address
def fetch_account_transactions(address, ledger_min, ledger_max):
    all_transactions = []
    marker = None

    while True:
        payload = {
            "method": "account_tx",
            "params": [
                {
                    "account": address,
                    "ledger_index_min": ledger_min,
                    "ledger_index_max": ledger_max,
                    "binary": False,
                    "limit": 500,
                    "forward": True,
                }
            ],
        }

        if marker:
            payload["params"][0]["marker"] = marker

        data = safe_post(payload)

        if not data:
            break  # exit if request failed

        result = data.get("result", {})
        transactions = result.get("transactions", [])
        all_transactions.extend(transactions)

        marker = result.get("marker")
        if not marker:
            break

        time.sleep(0.5)  # Gentle pacing between requests

    return all_transactions


# Helper: check if a transaction is a PFT payment
def is_pft_payment(tx):
    tx_data = tx.get("tx", {})
    if tx_data.get("TransactionType") != "Payment":
        return False
    amount = tx_data.get("Amount")
    if isinstance(amount, dict):
        return (
            amount.get("currency") == PFT_CURRENCY_CODE
            and amount.get("issuer") == ISSUER_ADDRESS
        )
    return False


active_addresses = set()


# Helper: update balances
def update_balances(balances, tx, active_addresses):
    tx_data = tx["tx"]
    amount_data = tx_data["Amount"]
    amount = float(amount_data["value"])

    sender = tx_data["Account"]
    receiver = tx_data["Destination"]
    active_addresses.update([sender, receiver])

    balances[sender] = balances.get(sender, 0) - amount
    balances[receiver] = balances.get(receiver, 0) + amount


# Helper: save daily balances
def save_balances_to_csv(day_number, balances, csv_writer):
    for address, balance in balances.items():
        if balance != 0:  # Save only active balances
            csv_writer.writerow([day_number, address, balance])


# Helper: save daily transfer counts
def save_transfer_count(day_number, transfer_count, csv_writer):
    csv_writer.writerow([day_number, transfer_count])


def insert_transfer(conn, tx, day_number):
    tx_data = tx["tx"]
    amount = float(tx_data["Amount"]["value"])
    sender = tx_data["Account"]
    receiver = tx_data["Destination"]
    tx_hash = tx_data["hash"]
    ledger_index = tx_data["ledger_index"]
    timestamp = tx_data.get("date", "")

    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO pft_transfers (tx_hash, day, ledger_index, sender, receiver, amount, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tx_hash) DO NOTHING;
            """,
                (
                    tx_hash,
                    day_number,
                    ledger_index,
                    sender,
                    receiver,
                    amount,
                    timestamp,
                ),
            )
        except Exception as e:
            print(f"DB insert failed for {tx_hash}: {e}")


# ---- MAIN ----


def main():
    conn = connect_db()
    create_table_if_not_exists(conn)

    start_day = load_checkpoint()  # Day 0 = April 26, 2024
    number_of_days = 368  # Start small, expand later
    balances = {}
    new_addresses = set()

    # Open CSVs
    with (
        open(BALANCES_CSV, mode="w", newline="") as balances_csvfile,
        open(TRANSFERS_CSV, mode="w", newline="") as transfers_csvfile,
        open(DAILY_ACTIVE_CSV, mode="w", newline="") as active_csvfile,
        open(CIRCULATING_SUPPLY_CSV, mode="w", newline="") as supply_csvfile,
    ):

        balances_writer = csv.writer(balances_csvfile)
        transfers_writer = csv.writer(transfers_csvfile)
        active_writer = csv.writer(active_csvfile)
        supply_writer = csv.writer(supply_csvfile)

        balances_writer.writerow(["day", "address", "balance"])
        transfers_writer.writerow(["day", "pft_transfer_count"])
        active_writer.writerow(["day", "active_address_count"])
        supply_writer.writerow(["day", "circulating_supply"])

        for day_offset in range(number_of_days):
            active_addresses = set()
            day_number = start_day + day_offset
            ledger_min, ledger_max = get_ledger_range_for_day(day_number)

            print(
                f"\nProcessing Day {day_number}: Ledgers {ledger_min} to {ledger_max}"
            )
            pft_transfer_count = 0

            # Track addresses for this day
            day_addresses = set(watchlist)

            for address in day_addresses:
                transactions = fetch_account_transactions(
                    address, ledger_min, ledger_max
                )

                for tx in transactions:
                    if is_pft_payment(tx):
                        tx_hash = tx["tx"]["hash"]
                        # print(f"PFT transfer found, {tx_hash}")
                        insert_transfer(conn, tx, day_number)

                        update_balances(balances, tx, active_addresses)
                        pft_transfer_count += 1

                        sender = tx["tx"]["Account"]
                        receiver = tx["tx"]["Destination"]

                        if sender not in watchlist:
                            new_addresses.add(sender)
                        if receiver not in watchlist:
                            new_addresses.add(receiver)

            # Expand watchlist
            watchlist.update(new_addresses)
            new_addresses.clear()

            # Save daily output
            save_balances_to_csv(day_number, balances, balances_writer)
            save_transfer_count(day_number, pft_transfer_count, transfers_writer)
            active_writer.writerow([day_number, len(active_addresses)])
            total_supply = sum(balance for balance in balances.values() if balance > 0)
            supply_writer.writerow([day_number, total_supply])

            save_checkpoint(day_number + 1)
            if (day_number + 1) % 10 == 0:
                print(f"✅ Completed {day_number + 1} days so far...")

            if (day_number + 1) % 25 == 0:
                print("⏸ Taking a short break to avoid overloading the server...")
                time.sleep(300)  # 5 minutes

    print(f"\nSaved all balances to {BALANCES_CSV}")
    print(f"Saved all transfer counts to {TRANSFERS_CSV}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
