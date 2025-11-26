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
from models.manufacturing_models import Equipment, Technician, Downtime, Maintenance

# ---------------- CONSTANT DATA ---------------- #
EQUIPMENT_TYPE = ["Press", "Milling", "Assembly_Robot", "Lathe"]
MANUFACTURERS = ["Mitsubishi", "ABB", "Siemens", "Bosch", "Makita", "DEWALT"]
CYCLE_DAYS = [20, 30, 60, 45, 90, 120]
LOCATIONS = ["Plant A", "Plant B", "Plant C", "Plant D", "Plant Z12"]
CRITICALITY = list(range(1,11))

DOWNTIME_TYPE = ["Scheduled", "Operator Error", "Mechanical", "Electrical"]
ROOT_CAUSE = ["Routine Check", "Power Loss", "Bearing Failure", "Overheating", "Misalignment"]
COMMENTS = ["Fixed promptly", "Monitoring required", "Replacement needed"]

MAINT_TYPE = ["Inspection", "Corrective", "Preventive"]
PARTS_REPLACED = ["Hydraulics", "None", "Motor", "Sensor", "Bearing"]
REMARKS = ["Needs follow-up", "Operational", "Issue resolved"]

LEVELS = ["Senior", "Assistant", "Junior"]

fake = Faker()
PROB_NEW = 0.30

def rand_date() -> datetime:
    return (datetime(2023, 1, 1) + timedelta(days=random.randint(0,600)))

def rand_end_date(start_date) -> datetime:
    return (start_date + timedelta(minutes=random.randint(20,800)))

def generate_random_phone_number():
    # Generate a 10-digit number with the first digit from 6-9
    number = str(random.randint(6, 9))
    for _ in range(9):
        number += str(random.randint(0, 9))
    return number

def init_from_csv_and_seed_db(config: Dict[str, Any],
                              ws_map: Dict[str, Any],
                              conn):
    """
    Equivalent to your initialize_from_csv() for retail:
    - Load CSVs into Sheets (if empty)
    - Seed SQLite tables from CSV
    """
    # EQUIPMENT
    dfe = init_sheet_from_csv_if_empty(ws_map["equipment"], config["csv_paths"]["equipment"])
    if dfe is not None:
        rows = []
        for _, row in dfe.iterrows():
            rows.append({
                "eq_id": row["Equipment_ID"],
                "name": row["Equipment_Name"],
                "etype": row["Equipment_Type"],
                "manufacturer": row["Manufacturer"],
                "install_date": row["Installation_Date"],
                "cycle_days": row["Maintenance_Cycle_Days"],
                "location": row["Location"],
                "capacity": row["Capacity_per_Hour"],
                "criticality": row["Criticality_Score"]
            })
        insert_many(conn, "mfg_equipments", rows)

    # TECHNICIAN
    dft = init_sheet_from_csv_if_empty(ws_map["technician"], config["csv_paths"]["technician"])
    if dft is not None:
        rows = []
        for _, row in dft.iterrows():
            rows.append({
                "tid": row["Technician_ID"],
                "name": row["Name"],
                "age": row["Age"],
                "phone": row["Phone"],
                "level": row["Level"]
            })
        insert_many(conn, "mfg_technicians", rows)

    # DOWNTIME & MAINTAINENCE 
    dfdown = init_sheet_from_csv_if_empty(ws_map["downtime"], config["csv_paths"]["downtime"])
    dfmain = init_sheet_from_csv_if_empty(ws_map["maintenance"], config["csv_paths"]["maintenance"])

    mem_rows = []
    if dfdown is not None and dfmain is not None:
        for i in range(min(len(dfdown), len(dfmain))):
            mem_rows.append({
                "downtime_id": dfdown.iloc[i]["Downtime_ID"],
                "maintenance_id": dfmain.iloc[i]["Maintenance_ID"]
            })
        insert_many(conn, "mfg_mem", mem_rows)

def load_mfg_memory(conn) -> (List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]):
    equipments = fetch_all(conn, "mfg_equipments")
    technicians = fetch_all(conn, "mfg_technicians")
    mem = fetch_all(conn, "mfg_mem")
    return equipments, technicians, mem

def get_or_create_equipment(equipments: List[Dict[str, Any]], conn) -> (Equipment, bool):
    if random.random() < PROB_NEW or len(equipments) == 0:
        next_num = (
            max(int(e["eq_id"][3:]) for e in equipments)
            + 1
            if equipments else 1
        )
        eid = f"EQT{next_num:03d}"

        e = Equipment(
            eq_id=eid,
            name=f"Machine_{next_num}",
            etype=random.choice(EQUIPMENT_TYPE),
            manufacturer=random.choice(MANUFACTURERS),
            install_date=rand_date(),
            cycle_days=random.choice(CYCLE_DAYS),
            location=random.choice(LOCATIONS),
            capacity=random.randint(80,500),
            criticality=random.randint(1,10)
        )
        data = e.__dict__
        insert_row(conn, "mfg_equipments", data)
        equipments.append(data)
        return e, True
    else:
        e = random.choice(equipments)
        return Equipment(**e), False

def get_or_create_tech(tech: List[Dict[str, Any]], conn) -> (Technician, bool):
    if random.random() < PROB_NEW or len(tech) == 0:
        next_num = (
            max(int(t["tid"][1:]) for t in tech)
            + 1
            if tech else 1
        )
        tid = f"T{next_num:03d}"
        t = Technician(
            tid=tid,
            name=fake.name(),
            age=random.randint(20,60),
            phone=generate_random_phone_number(),
            level=random.choice(LEVELS)
        )
        data = t.__dict__
        insert_row(conn, "mfg_technicians", data)
        tech.append(data)
        return t, True
    else:
        t = random.choice(tech)
        return Technician(**t), False

def generate_records(equipments, tech, mfg_mem, conn):
    equip, new_e = get_or_create_equipment(equipments, conn)
    techs, new_t = get_or_create_tech(tech, conn)

    next_num = (
        max(int(d["downtime_id"][2:]) for d in mfg_mem)
        + 1
        if mfg_mem else 1
    )
    dtid = f"DT{next_num:03d}"
    next_num = (
        max(int(m["maintenance_id"][2:]) for m in mfg_mem)
        + 1
        if mfg_mem else 1
    )
    mid = f"MT{next_num:03d}"

    mem_row = {"downtime_id": dtid, "maintenance_id": mid} 
    insert_row(conn, "mfg_mem", mem_row)
    mfg_mem.append(mem_row)

    start= rand_date()
    end = rand_end_date(start)
    dur = int((end - start).total_seconds() / 60)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    downtime = Downtime(
        dt_id=dtid,
        eq_id=equip.eq_id,
        start=start_str,
        end=end_str,
        duration=dur,
        root=random.choice(ROOT_CAUSE),
        tech=techs.tid,
        comments=random.choice(COMMENTS)
    )

    main = Maintenance(
        mt_id=mid,
        eq_id=equip.eq_id,
        date=rand_date(),
        mtype=random.choice(MAINT_TYPE),
        parts=random.choice(PARTS_REPLACED),
        tech= techs.tid,
        cost=round(random.uniform(300.0, 5000.0), 2),
        mttr=random.randint(60,400),
        remarks=random.choice(REMARKS)
    )

    return equip, techs, downtime, main, new_e, new_t

