import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

from faker import Faker
import pandas as pd

from core.db_utils import (
    fetch_all,
    insert_row,
    insert_many,
)
from core.sheets_append import SheetBuffer, init_sheet_from_csv_if_empty
from models.retail_models import Product, Store, Sale, Inventory


# ---------------- CONSTANT DATA (COPY EXACTLY FROM YOUR ORIGINAL FILE) ---------------- #
CATEGORY = ["Beverages", "Snacks", "Dairy", "Personal Care"]
SUB_CATEGORY = {
    "Beverages": ["Tea", "Juice", "Soda"],
    "Snacks": ["Nuts", "Chips", "Cookies"],
    "Dairy": ["Cheese", "Butter", "Milk"],
    "Personal Care": ["Soap", "Shampoo", "Lotion"]
}
BRAND = ["General Goods", "East End Shop", "Blue Mountain", "EcoFoods", "DailyMart", "FreshCO"]
STORE_TYPE = ["Small", "Medium", "Large", "Franchise"]
STORE_NAME = [
    "Prime Retail Hub", "CityMart Superstore", "GreenLeaf Market", "Daily Basket Outlet",
    "UrbanFresh Store", "Metro Value Center", "Sunrise Grocery Point", "FreshWorld Hypermart",
    "QuickPick Convenience", "EcoShop Department Store", "ValueTown Retail"
]
LOCATION = [
    "New Delhi", "Mumbai", "Bengaluru", "Chennai", "Kolkata", "Hyderabad", "Pune", "Jaipur",
    "Kochi", "Ahmedabad", "Lucknow", "Chandigarh", "Bhopal", "Indore", "Surat",
    "Visakhapatnam", "Nagpur", "Gurugram", "Noida", "Mysore", "Coimbatore", "Thiruvananthapuram"
]

fake = Faker()
PROB_NEW = 0.30


def rand_date() -> str:
    return (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d")


def init_from_csv_and_seed_db(config: Dict[str, Any],
                              ws_map: Dict[str, Any],
                              conn):
    """
    Equivalent to your initialize_from_csv() for retail:
    - Load CSVs into Sheets (if empty)
    - Seed SQLite tables from CSV
    """
    # PRODUCT
    dfp = init_sheet_from_csv_if_empty(ws_map["product"], config["csv_paths"]["product"])
    if dfp is not None:
        rows = []
        for _, row in dfp.iterrows():
            rows.append({
                "pid": row["Product_ID"],
                "name": row["Product_Name"],
                "category": row["Category"],
                "subcat": row["Sub_Category"],
                "brand": row["Brand"],
                "cost": float(row["Cost_Price"]),
                "selling": float(row["Selling_Price"]),
                "shelf_life": int(row["Shelf_Life_Days"])
            })
        insert_many(conn, "retail_products", rows)

    # STORE
    dfs = init_sheet_from_csv_if_empty(ws_map["store"], config["csv_paths"]["store"])
    if dfs is not None:
        rows = []
        for _, row in dfs.iterrows():
            rows.append({
                "sid": row["Store_ID"],
                "name": row["Store_Name"],
                "location": row["Location"],
                "manager": row["Manager_Name"],
                "stype": row["Store_Type"]
            })
        insert_many(conn, "retail_stores", rows)

    # SALES & INVENTORY → just sheet, plus retail_mem
    dfsales = init_sheet_from_csv_if_empty(ws_map["sales"], config["csv_paths"]["sales"])
    dfinv = init_sheet_from_csv_if_empty(ws_map["inventory"], config["csv_paths"]["inventory"])

    mem_rows = []
    if dfsales is not None and dfinv is not None:
        for i in range(min(len(dfsales), len(dfinv))):
            mem_rows.append({
                "sale_id": dfsales.iloc[i]["Sale_ID"],
                "inv_id": dfinv.iloc[i]["Inventory_ID"]
            })
        insert_many(conn, "retail_mem", mem_rows)


def load_retail_memory(conn) -> (List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]):
    products = fetch_all(conn, "retail_products")
    stores = fetch_all(conn, "retail_stores")
    mem = fetch_all(conn, "retail_mem")
    return products, stores, mem


def get_or_create_product(products: List[Dict[str, Any]], conn) -> (Product, bool):
    if random.random() < PROB_NEW or len(products) == 0:
        next_num = (
            max(int(p["pid"][1:]) for p in products)
            + 1
            if products else 1
        )
        pid = f"P{next_num:04d}"
        cat = random.choice(CATEGORY)
        sub = random.choice(SUB_CATEGORY[cat])

        p = Product(
            pid=pid,
            name=f"{sub}_{next_num}",
            category=cat,
            subcat=sub,
            brand=random.choice(BRAND),
            cost=round(random.uniform(20.0, 200.0), 2),
            selling=round(random.uniform(200.0, 800.0), 2),
            shelf_life=random.randint(60, 365)
        )
        data = p.__dict__
        insert_row(conn, "retail_products", data)
        products.append(data)
        return p, True
    else:
        d = random.choice(products)
        return Product(**d), False


def get_or_create_store(stores: List[Dict[str, Any]], conn) -> (Store, bool):
    if random.random() < PROB_NEW or len(stores) == 0:
        next_num = (
            max(int(s["sid"][3:]) for s in stores)
            + 1
            if stores else 1
        )
        sid = f"STR{next_num:03d}"
        s = Store(
            sid=sid,
            name=random.choice(STORE_NAME),
            location=random.choice(LOCATION),
            manager=fake.name(),
            stype=random.choice(STORE_TYPE)
        )
        data = s.__dict__
        insert_row(conn, "retail_stores", data)
        stores.append(data)
        return s, True
    else:
        d = random.choice(stores)
        return Store(**d), False


def generate_records(products, stores, retail_mem, conn):
    product, new_p = get_or_create_product(products, conn)
    store, new_s = get_or_create_store(stores, conn)

    next_num = (
        max(int(sl["sale_id"][1:]) for sl in retail_mem)
        + 1
        if retail_mem else 1
    )
    saleid = f"S{next_num:04d}"
    next_num = (
        max(int(i["sale_id"][1:]) for i in retail_mem)
        + 1
        if retail_mem else 1
    )
    invid = f"I{next_num:04d}"

    mem_row = {"sale_id": saleid, "inv_id": invid}
    insert_row(conn, "retail_mem", mem_row)
    retail_mem.append(mem_row)

    units = random.randint(1, 20)
    discount = round(random.uniform(0.10, 0.50), 2)
    final_price = round(product.selling - discount, 2)
    revenue = round(units * final_price, 2)

    sale = Sale(
        sale_id=saleid,
        pid=product.pid,
        sid=store.sid,
        date=rand_date(),
        units=units,
        discount=discount,
        final_price=final_price,
        revenue=revenue
    )

    opening = random.randint(50, 200)
    received = random.randint(10, 50)
    sold = units
    closing = opening + received - sold

    inv = Inventory(
        inv_id=invid,
        pid=product.pid,
        opening=opening,
        receieved=received,
        sold=sold,
        closing=closing
    )

    return product, store, sale, inv, new_p, new_s
