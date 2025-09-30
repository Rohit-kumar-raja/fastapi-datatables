from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, select
import random
from faker import Faker
from datatables import DataTablesResponse, DataTables, DataTablesRequest
from pydantic import BaseModel

# ----------------------
# Database setup
# ----------------------
DATABASE_URL = "sqlite+aiosqlite:///./students.db"

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()


def get_db():
    yield async_session()


# ----------------------
# Models
# ----------------------
class Student(Base):
    __tablename__ = "students"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    age = Column(Integer, nullable=False)
    email = Column(String, nullable=False, unique=True)


class StudentSchema(BaseModel):
    id: int
    name: str
    age: int


# ----------------------
# FastAPI app
# ----------------------
app = FastAPI()
faker = Faker()


# ----------------------
# Create tables on startup
# ----------------------
@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ----------------------
# Insert 1000 random students
# ----------------------
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


# ----------------------
# Get all students (optional)
# ----------------------
@app.post("/students", response_model=DataTablesResponse[list[StudentSchema]])
async def get_students(
    datatable_requst: DataTablesRequest, db: AsyncSession = Depends(get_db)
):
    stm = select(Student)
    datatable = DataTables(db, Student, stm)
    return await datatable.process(datatable_requst)
