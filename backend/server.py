from fastapi import FastAPI, HTTPException
from datetime import date

from tornado.gen import Return

import db_helper
from typing import List
from pydantic import BaseModel
app = FastAPI()
class Expense(BaseModel):
    amount: float
    category: str
    notes: str

class DateRange(BaseModel):
    start_date: date
    end_date: date
@app.get("/expenses/{expense_date}", response_model=List[Expense])
def get_expenses(expense_date: date):
    expenses = db_helper.fetch_expenses_for_date(expense_date)
    if expenses is None:
        raise HTTPException(status_code=500, detail="No data found")
    return expenses

@app.post("/expenses/{expense_date}")
def add_or_update_expense(expense_date: date,expense: List[Expense]):
    db_helper.delete_expense_for_date(expense_date)
    for expense in expense:
        db_helper.insert_expense(expense_date, expense.amount, expense.category, expense.notes)
    return {"Message": "Expenses updated successfully"}

@app.post("/analytics/")
def get_analytics(date_range:DateRange):
    data = db_helper.fetch_expense_summary(date_range.start_date, date_range.end_date)
    if data is None:
        raise HTTPException(status_code=500, detail="No data found")
    total = sum([row['total'] for row in data])
    breakdown = {}
    for row in data:
        percentage = (row['total'] / total) * 100 if total != 0 else 0
        breakdown[row['category']] = {
            "total": row['total'],
            "percentage": percentage
        }
    return breakdown