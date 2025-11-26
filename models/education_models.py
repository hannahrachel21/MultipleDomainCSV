from dataclasses import dataclass

# Dataclasses (used internally, same as your logic)
@dataclass
class Student:
    sid: str
    name: str
    age: int
    gender: str
    course: str
    enroll_date: str
    style: str
    grade: str

@dataclass
class Progress:
    rid: str
    sid: str
    mid: str
    mname: str
    completion: int
    time_spent: int
    quiz: int
    difficulty: int
    date: str

@dataclass
class ResourceUsage:
    rid: str
    sid: str
    rtype: str
    spent: int
    status: str
    adate: str

@dataclass
class Module:
    mid: str
    mname: str
    cname: str
    diff: int
    mtype: str