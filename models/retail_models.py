from dataclasses import dataclass

# Dataclasses (used internally, same as your logic)
@dataclass
class Product:
    pid: str
    name: str
    category: str
    subcat: str
    brand: str
    cost: float
    selling: float
    shelf_life: int


@dataclass
class Store:
    sid: str
    name: str
    location: str
    manager: str
    stype: str


@dataclass
class Sale:
    sale_id: str
    pid: str
    sid: str
    date: str
    units: int
    discount: float
    final_price: float
    revenue: float


@dataclass
class Inventory:
    inv_id: str
    pid: str
    opening: int
    receieved: int
    sold: int
    closing: int
