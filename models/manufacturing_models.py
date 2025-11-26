from dataclasses import dataclass

# Dataclasses (used internally, same as your logic)
@dataclass
class Equipment:
    eq_id: str
    name: str
    etype: str
    manufacturer: str
    install_date: str
    cycle_days: int
    location: str
    capacity: int
    criticality: int

@dataclass
class Downtime:
    dt_id: str
    eq_id: str
    start: str
    end: str
    duration: int
    root: str
    tech: str
    comments: str

@dataclass
class Maintenance:
    mt_id: str
    eq_id: str
    date: str
    mtype: str
    parts: str
    tech: str
    cost: int
    mttr: int
    remarks: str

@dataclass
class Technician:
    tid: str
    name: str
    age: int
    phone: str
    level: str