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
from models.education_models import  Student, Progress, ResourceUsage, Module

GENDER = ["Male", "Female"]
COURSE_ENROLLED = ["English", "Maths", "Science", "Social Science", "Computer"]
LEARNING_STYLE = ["Visual", "Kinesthetic", "Auditory", "Reading/Writing"]
PRIOR_GRADE = ["A", "B+", "B", "C+", "C"]

RESOURCE_TYPE = ["Quiz", "Video", "PDF", "Assignment"]
COMPLETION_STATUS = ["In Progress", "Not Started", "Completed"]
COURSE_NAME = ["English", "Maths", "Science", "Social Science", "Computer"]
MODULE_TYPE = ["Quiz", "Video", "PDF", "Assignment"]

fake = Faker()
PROB_NEW = 0.30

# ID POOLS
student_ids = [f"S{str(i).zfill(4)}" for i in range(1, 5000)]
record_ids  = [f"R{str(i).zfill(4)}" for i in range(1, 8000)]
resource_ids = [f"RU{str(i).zfill(4)}" for i in range(1, 8000)]
module_ids = [f"M{str(i).zfill(3)}" for i in range(1, 2000)]

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
    # STUDENT
    dfst = init_sheet_from_csv_if_empty(ws_map["student"], config["csv_paths"]["student"])
    if dfst is not None:
        rows = []
        for _, row in dfst.iterrows():
            rows.append({
                "sid": row["Student_ID"],
                "name": row["Name"],
                "age": row["Age"],
                "gender": row["Gender"],
                "course": row["Course_Enrolled"],
                "enroll_date": row["Enrollment_Date"],
                "style": row["Learning_Style"],
                "grade": row["Prior_Grade"]
            })
        insert_many(conn, "edu_students", rows)

    # MODULE
    dfm = init_sheet_from_csv_if_empty(ws_map["module"], config["csv_paths"]["module"])
    if dfm is not None:
        rows = []
        for _, row in dfm.iterrows():
            rows.append({
                "mid": row["Module_ID"],
                "mname": row["Module_Name"],
                "cname": row["Course_Name"],
                "diff": row["Difficulty_Level"],
                "mtype": row["Module_Type"]
            })
        insert_many(conn, "edu_modules", rows)

    # LEARNING_PROGRESS & RESOURCE_USAGE
    dfp = init_sheet_from_csv_if_empty(ws_map["progress"], config["csv_paths"]["progress"])
    dfr = init_sheet_from_csv_if_empty(ws_map["resource"], config["csv_paths"]["resource"])

    mem_rows = []
    if dfp is not None and dfr is not None:
        for i in range(min(len(dfp), len(dfr))):
            mem_rows.append({
                "record_id": dfp.iloc[i]["Record_ID"],
                "resource_id": dfr.iloc[i]["Resource_ID"]
            })
        insert_many(conn, "edu_mem", mem_rows)

def load_edu_memory(conn) -> (List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]):
    students = fetch_all(conn, "edu_students")
    modules = fetch_all(conn, "edu_modules")
    mem = fetch_all(conn, "edu_mem")
    return students, modules, mem

def get_or_create_student(students: List[Dict[str, Any]], conn) -> (Student, bool):
    if random.random() < PROB_NEW or len(students) == 0:
        next_num = (
            max(int(s["sid"][1:]) for s in students)
            + 1
            if students else 1
        )
        stid = f"S{next_num:04d}"
        gender = random.choice(GENDER)

        st = Student(
            sid=stid,
            name= fake.first_name_male() if gender == "Male" else fake.first_name_female(),
            age= random.randint(14,18),
            gender=gender,
            course=random.choice(COURSE_ENROLLED),
            enroll_date=rand_date(),
            style=random.choice(LEARNING_STYLE),
            grade= random.choice(PRIOR_GRADE)
        )
        data = st.__dict__
        insert_row(conn, "edu_students", data)
        students.append(data)
        return st, True
    else:
        st = random.choice(students)
        return Student(**st), False

def get_or_create_module(modules: List[Dict[str, Any]], conn) -> (Module, bool):
    if random.random() < PROB_NEW or len(modules) == 0:
        next_num = (
            max(int(m["mid"][1:]) for m in modules)
            + 1
            if modules else 1
        )
        mid = f"M{next_num:03d}"
        m = Module(
            mid= mid,
            mname= f"Module{next_num}",
            cname= random.choice(COURSE_NAME),
            diff= random.randint(1,10),
            mtype=random.choice(MODULE_TYPE)
        )
        data = m.__dict__
        insert_row(conn, "edu_modules", data)
        modules.append(data)
        return m, True
    else:
        m = random.choice(modules)
        return Module(**m), False

def generate_records(students, modules, edu_mem, conn):
    student, new_s = get_or_create_student(students, conn)
    module, new_m = get_or_create_module(modules, conn)

    next_num = (
        max(int(rc["record_id"][1:]) for rc in edu_mem)
        + 1
        if edu_mem else 1
    )
    rcid = f"R{next_num:04d}"
    next_num = (
        max(int(rs["resource_id"][2:]) for rs in edu_mem)
        + 1
        if edu_mem else 1
    )
    rsid = f"RU{next_num:04d}"

    mem_row = {"record_id": rcid, "resource_id": rsid}
    insert_row(conn, "edu_mem", mem_row)
    edu_mem.append(mem_row)

    progress = Progress(
        rid= rcid,
        sid= student.sid,
        mid= module.mid,
        mname= module.mname,
        completion= random.randint(10,100),
        time_spent= random.randint(60,400),
        quiz= random.randint(1,100),
        difficulty=random.randint(1,5),
        date= rand_date()
    )

    resource = ResourceUsage(
        rid= rsid,
        sid= student.sid,
        rtype= random.choice(RESOURCE_TYPE),
        spent= random.randint(5,300),
        status= random.choice(COMPLETION_STATUS),
        adate= rand_date()
    )
    return student, module, progress, resource, new_s, new_m

