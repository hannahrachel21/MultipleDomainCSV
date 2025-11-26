import argparse
import time
import yaml

from core.db_utils import (
    get_connection,
    init_retail_schema,
    init_mfg_schema,
    init_edu_schema,
)
from core.sheets_append import get_sheets_client, load_worksheets, SheetBuffer


def load_config(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run_retail(config):
    from domains import retail_simulator as sim

    conn = get_connection(config["sqlite"]["db_path"])
    init_retail_schema(conn)

    client, spread = get_sheets_client(config["service_json"], config["sheet_url"])
    ws_map = load_worksheets(spread, config["worksheets"])

    # Initialize from CSV if sheets empty & seed DB
    sim.init_from_csv_and_seed_db(config, ws_map, conn)

    # Load in-memory state
    products, stores, retail_mem = sim.load_retail_memory(conn)

    buf_prod = SheetBuffer(ws_map["product"], config["buffer_size"])
    buf_store = SheetBuffer(ws_map["store"], config["buffer_size"])
    buf_sales = SheetBuffer(ws_map["sales"], config["buffer_size"])
    buf_inv = SheetBuffer(ws_map["inventory"], config["buffer_size"])

    print("Retail simulation started... Ctrl+C to stop.")
    try:
        while True:
            p, s, sale, inv, new_p, new_s = sim.generate_records(
                products, stores, retail_mem, conn
            )

            if new_p:
                buf_prod.add(list(p.__dict__.values()))
            if new_s:
                buf_store.add(list(s.__dict__.values()))
            buf_sales.add(list(sale.__dict__.values()))
            buf_inv.add(list(inv.__dict__.values()))

            print(f"Added sale {sale.sale_id} for product {p.pid} at store {s.sid}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        buf_prod.flush()
        buf_store.flush()
        buf_sales.flush()
        buf_inv.flush()
        print("Stopped and flushed all buffers.")


def run_manufacturing(config):
    from domains import manufacturing_simulator as sim

    conn = get_connection(config["sqlite"]["db_path"])
    init_mfg_schema(conn)

    client, spread = get_sheets_client(config["service_json"], config["sheet_url"])
    ws_map = load_worksheets(spread, config["worksheets"])

    sim.init_from_csv_and_seed_db(config, ws_map, conn)
    equipments, technicians, mfg_mem = sim.load_mfg_memory(conn)

    buf_equip = SheetBuffer(ws_map["equipment"], config["buffer_size"])
    buf_down = SheetBuffer(ws_map["downtime"], config["buffer_size"])
    buf_maint = SheetBuffer(ws_map["maintenance"], config["buffer_size"])
    buf_tech = SheetBuffer(ws_map["technician"], config["buffer_size"])

    print("Manufacturing simulation started... Ctrl+C to stop.")
    try:
        while True:
            eq, tech, down, maint, new_e, new_t = sim.generate_records(
                equipments, technicians, mfg_mem, conn
            )

            if new_e:
                buf_equip.add(list(eq.__dict__.values()))
            if new_t:
                buf_tech.add(list(tech.__dict__.values()))
            buf_down.add(list(down.__dict__.values()))
            buf_maint.add(list(maint.__dict__.values()))

            print(f"DT {down.dt_id} | MT {maint.mt_id} | EQ {eq.eq_id} | TECH {tech.tid}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        buf_equip.flush()
        buf_tech.flush()
        buf_down.flush()
        buf_maint.flush()
        print("Stopped & flushed.")


def run_education(config):
    from domains import education_simulator as sim

    conn = get_connection(config["sqlite"]["db_path"])
    init_edu_schema(conn)

    client, spread = get_sheets_client(config["service_json"], config["sheet_url"])
    ws_map = load_worksheets(spread, config["worksheets"])

    sim.init_from_csv_and_seed_db(config, ws_map, conn)
    students, modules, edu_mem = sim.load_edu_memory(conn)

    buf_student = SheetBuffer(ws_map["student"], config["buffer_size"])
    buf_module = SheetBuffer(ws_map["module"], config["buffer_size"])
    buf_progress = SheetBuffer(ws_map["progress"], config["buffer_size"])
    buf_resource = SheetBuffer(ws_map["resource"], config["buffer_size"])

    print("Education simulation started... Ctrl+C to stop.")
    try:
        while True:
            stu, mod, prog, res, new_s, new_m = sim.generate_records(
                students, modules, edu_mem, conn
            )

            if new_s:
                buf_student.add(list(stu.__dict__.values()))
            if new_m:
                buf_module.add(list(mod.__dict__.values()))
            buf_progress.add(list(prog.__dict__.values()))
            buf_resource.add(list(res.__dict__.values()))

            print(f"REC {prog.rid} | RES {res.rid} | STUD {stu.sid} | MOD {mod.mid}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        buf_student.flush()
        buf_module.flush()
        buf_progress.flush()
        buf_resource.flush()
        print("Stopped & flushed.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", choices=["retail", "manufacturing", "education"], required=True)
    parser.add_argument("--config", type=str, help="Path to YAML config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)

    if args.domain == "retail":
        run_retail(config)
    elif args.domain == "manufacturing":
        run_manufacturing(config)
    elif args.domain == "education":
        run_education(config)


if __name__ == "__main__":
    main()
