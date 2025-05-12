"""
Author: Jelani Jenkins & Clint Scott
Date: 05/10/2025
Assignment: Module 11.1 - Report Creator

Description:
------------
This script connects to a MySQL database and generates three reports in text file format
to assist with business decisions for Bacchus Winery. It reads credentials from a .env file,
retrieves data from the database using SQL queries, and writes the results to .txt files.

Reports Generated:
1. Pending Wine Orders
2. Low Supply Inventory
3. Employee Weekly Hours
"""

import mysql.connector
from datetime import datetime
from dotenv import dotenv_values
from pathlib import Path
import os

# Set current and base directories to locate .env and output folders/files
CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = Path(__file__).resolve().parent.parent
ENV = Path(os.path.join(BASE_DIR, ".env"))

# Load environment variables from .env file
if os.path.exists(ENV):
    secrets = dotenv_values(ENV)
else:
    raise FileNotFoundError("No .env file found.")

def reconnect_to_db():
    """
    Establishes and returns a new connection to the MySQL database using credentials
    loaded from the .env file.
    """
    return mysql.connector.connect(
        user=secrets["USER"],
        password=secrets["PASSWORD"],
        host=secrets["HOST"],
        database=secrets["DATABASE"],
        raise_on_warnings=True
    )

def write_report(file_name, title, rows, formatter):
    """
    Writes a report to the specified file.

    Args:
        file_name (str): The name of the output file.
        title (str): The report header to write.
        rows (list): Query result rows to write.
        formatter (func): A function that takes a row and returns a formatted string.
    """
    output_path = os.path.join(CURRENT_DIR, file_name)
    with open(output_path, "w") as writer:
        writer.write(f"***** {title} *****\n")
        for row in rows:
            writer.write(formatter(row) + "\n")
    print(f"{title} report written: {len(rows)} records")
    print(f"File created: {output_path}\n")

def generate_reports():
    """
    Generates three separate business reports:
    1. Pending Wine Orders
    2. Low Supply Inventory
    3. Employee Weekly Hours

    Each report is written to a uniquely named .txt file in the current directory.
    """
    conn = reconnect_to_db()
    cursor = conn.cursor()

    # ----------------------
    # Report 1: Pending Wine Orders
    # ----------------------
    cursor.execute("""
        SELECT Distributor.Name, Wine.Name, WineOrders.Quantity, WineOrders.OrderDate
        FROM WineOrders
        JOIN Distributor ON WineOrders.DistributorID = Distributor.DistributorID
        JOIN Wine ON WineOrders.WineID = Wine.WineID
        WHERE WineOrders.OrderStatus = 'Pending';
    """)
    rows = cursor.fetchall()
    write_report(
        "bacchus_pending_wine_report.txt",
        "Pending Wine Orders",
        rows,
        lambda r: f"Distributor: {r[0]}, Wine: {r[1]}, Quantity: {r[2]}, Ordered On: {r[3]}"
    )

    # ----------------------
    # Report 2: Low Supply Inventory
    # ----------------------
    cursor.execute("""
        SELECT SupplyType.Description, SupplyInventory.QuantityOnHand
        FROM SupplyInventory
        JOIN SupplyType ON SupplyInventory.SupplyTypeID = SupplyType.SupplyTypeID
        WHERE SupplyInventory.QuantityOnHand < 100;
    """)
    rows = cursor.fetchall()
    write_report(
        "bacchus_low_supply_inventory_report.txt",
        "Low Supply Inventory",
        rows,
        lambda r: f"Supply: {r[0]}, Quantity On Hand: {r[1]}"
    )

    # ----------------------
    # Report 3: Employee Weekly Hours
    # ----------------------
    cursor.execute("""
        SELECT Employee.Name, EmployeeHours.Week, EmployeeHours.HoursWorked
        FROM EmployeeHours
        JOIN Employee ON EmployeeHours.EmployeeID = Employee.EmployeeID;
    """)
    rows = cursor.fetchall()
    write_report(
        "bacchus_employee_weekly_hours_report.txt",
        "Employee Weekly Hours",
        rows,
        lambda r: f"Employee: {r[0]}, Week: {r[1]}, Hours Worked: {r[2]}"
    )

    # Clean up DB resources
    cursor.close()
    conn.close()

    print("All reports generated successfully.")

if __name__ == "__main__":
    generate_reports()
