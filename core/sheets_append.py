import os
from typing import Dict, List, Any
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


class SheetBuffer:
    def __init__(self, worksheet, buffer_size: int = 5):
        self.ws = worksheet
        self.buffer_size = buffer_size
        self.rows: List[List[str]] = []

    def add(self, row: List[Any]):
        self.rows.append([str(x) for x in row])
        if len(self.rows) >= self.buffer_size:
            self.flush()

    def flush(self):
        if self.rows:
            self.ws.append_rows(self.rows)
            self.rows = []


def get_sheets_client(service_json: str, sheet_url: str):
    creds = Credentials.from_service_account_file(
        service_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    spread = client.open_by_url(sheet_url)
    return client, spread


def load_worksheets(spread, worksheet_map: Dict[str, str]):
    """
    worksheet_map: {"product": "Product_Master", ...}
    returns: {"product": Worksheet, ...}
    """
    ws = {}
    for key, name in worksheet_map.items():
        ws[key] = spread.worksheet(name)
    return ws


def init_sheet_from_csv_if_empty(ws, csv_path: str):
    """
    If the worksheet is effectively empty (<= 1 row), load the CSV into it.
    """
    values = ws.get_all_values()
    if len(values) <= 1 and os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
        return df
    return None
