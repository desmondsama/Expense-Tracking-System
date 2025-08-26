from logging_setup import setup_logger

import mysql.connector
from contextlib import contextmanager

logger = setup_logger('db_helper')

@contextmanager
def get_db_cursor(commit=False):
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='expense_manager'
    )

    cursor = connection.cursor(dictionary=True)
    yield cursor
    if commit:
        connection.commit()
    cursor.close()
    connection.close()


def fetch_expenses_for_date(expense_date):
    logger.info(f"Fetching expenses for date called with {expense_date}")
    with get_db_cursor() as cursor:
        cursor.execute("SELECT * FROM expenses WHERE expense_date=%s", (expense_date,))
        expenses = cursor.fetchall()
        for expense in expenses:
            print(expense)
        return expenses  # Add this line!


def delete_expense_for_date(expense_date):
    logger.info(f"Fetching expenses for date called with {expense_date}")
    with get_db_cursor(commit=True) as cursor:
        cursor.execute("DELETE from expenses WHERE expense_date = %s", (expense_date,))


def insert_expense(expense_date, amount, category, notes):
    logger.info(f"Inserting expense for date called with {expense_date}, amount {amount}, category {category}, notes {notes}")
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            "INSERT INTO expenses (expense_date, amount, category, notes) VALUES (%s, %s, %s, %s)",
            (expense_date, amount, category, notes)
        )


def fetch_expense_summary(start_date, end_date):
    logger.info(f"Fetching expense summary for date called with {start_date}, and {end_date}")
    with get_db_cursor() as cursor:
        cursor.execute(
            '''SELECT category, SUM(amount) as total
                        FROM expenses WHERE expense_date BETWEEN %s and %s
                        GROUP BY category;''',
                       (start_date, end_date)
        )
        data = cursor.fetchall()
        return data
#import os
if __name__ == "__main__":
    expenses = fetch_expenses_for_date("2024-08-01")
    print(expenses)
    # project_root = os.path.join(os.path.dirname(__file__), '..')
    # print("**Project Root**:", project_root)
    summary = fetch_expense_summary('2024-08-01', '2024-08-05')
    for record in summary:
       print(record)