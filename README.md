
# FastAPI DataTables

A simple integration of **[DataTables.js](https://datatables.net/)** (or any standard DataTables-like frontend library) with **FastAPI** and **SQLAlchemy**.

This package makes it easy to implement:

* **Server-side pagination**
* **Sorting**
* **Searching (deep search supported)**
* **Filtering**
* **Transforming query results**

Perfect for handling **large datasets** efficiently while keeping your API clean and async-friendly.

---

## 🚀 Installation

```bash
pip install fp-datatables
```

---

## 📖 Quick Start Example

### 1. Setup database and models

```python
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, select
from datatables import DataTables, DataTablesRequest, DataTablesResponse
from pydantic import BaseModel

DATABASE_URL = "sqlite+aiosqlite:///./students.db"

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# Dependency for DB session
async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session

# Model
class Student(Base):
    __tablename__ = "students"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    age = Column(Integer, nullable=False)
    email = Column(String, nullable=False, unique=True)

# Schema
class StudentSchema(BaseModel):
    id: int
    name: str
    age: int
```

---

### 2. Setup FastAPI app

```python
app = FastAPI()

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
```

---

### 3. Insert test data

```python
import random
from faker import Faker

faker = Faker()

@app.get("/insert_students")
async def insert_students():
    async with async_session() as session:
        students = [
            Student(
                name=faker.name(),
                age=random.randint(18, 25),
                email=faker.unique.email(),
            )
            for _ in range(1000)
        ]
        session.add_all(students)
        await session.commit()
    return {"message": "1000 random students inserted successfully!"}
```

---

### 4. Datatables API endpoint

```python
@app.post("/students", response_model=DataTablesResponse[list[StudentSchema]])
async def get_students(
    request: DataTablesRequest, db: AsyncSession = Depends(get_db)
):
    stmt = select(Student)
    datatable = DataTables(db, Student, stmt)
    return await datatable.process(request)
```

---

## ⚡ Frontend Usage

You can use **DataTables.js** (or any DataTables-compatible frontend) to consume this API.
Example DataTables setup (jQuery):

```javascript
$('#studentsTable').DataTable({
    serverSide: true,
    processing: true,
    ajax: {
        url: "/students",
        type: "POST",
        contentType: "application/json",
        data: function (d) {
            return JSON.stringify(d);
        }
    },
    columns: [
        { data: "id" },
        { data: "name" },
        { data: "age" }
    ]
});
```

---

## ✨ Features

* Async-first (built on **FastAPI + SQLAlchemy 2.0 async**)
* **Pagination, sorting, searching** out of the box
* Works with any frontend datatable library (DataTables.js, PrimeVue DataTable, etc.)
* Supports **deep field search** (`relation.field`)
* Easy integration with Pydantic models

---

## 🛠️ Roadmap

* [ ] Support for joins and relationships
* [ ] More advanced filters
* [ ] Examples for Vue/React/Angular integration

---

## 📜 License

MIT License © 2025 [Rohit Kumar](https://github.com/Rohit-kumar-raja)

---

👉 Would you like me to also add an **"Advanced Usage"** section (with filtering, deep search `student.classroom.name`, and custom column transformations) so your README looks more complete for real-world use cases?
