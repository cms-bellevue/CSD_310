"""
Author: Jelani Jenkins & Clint Scott
Date: 05/04/2025
Assignment: Module 10.1 - Data Import Script

write the .sql script to create the tables in MySQL, and populate each with at least 6 records
(fewer if noted in the case study, for example only three suppliers noted at Bacchus Winery). 
Write a python script that displays the data in each table (remember the movies example?),
and take a screenshot of the results of the script that displays the data in each table. 

"""

import mysql.connector
from dotenv import dotenv_values
from pathlib import Path
import os, sys

BASE_DIR = Path(__file__).resolve().parent.parent
ENV = Path(os.path.join(BASE_DIR, ".env"))

if os.path.exists(ENV):
    secrets = dotenv_values(ENV)
else:
    sys.exit("No .env file found.")

def reconnect_to_db():
    return mysql.connector.connect(**{"user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True})

def view_joined_data():
    conn = reconnect_to_db()
    cursor = conn.cursor()

    print("\n--- Employee with Department (INNER JOIN) ---")
    cursor.execute("""
        SELECT e.EmployeeID, e.Name, d.Name AS Department, e.Position
        FROM Employee e
        INNER JOIN Department d ON e.DepartmentID = d.DepartmentID;
    """)
    for row in cursor.fetchall():
        print(row)

    print("\n--- Employee Hours with Names (LEFT JOIN) ---")
    cursor.execute("""
        SELECT eh.RecordID, e.Name, eh.Week, eh.HoursWorked
        FROM EmployeeHours eh
        LEFT JOIN Employee e ON eh.EmployeeID = e.EmployeeID;
    """)
    for row in cursor.fetchall():
        print(row)

    print("\n--- Supply Shipments with Supplier and Supply Type (INNER JOIN) ---")
    cursor.execute("""
        SELECT ss.ShipmentID, s.Name AS Supplier, st.Description AS SupplyType, ss.Quantity
        FROM SupplyShipment ss
        JOIN Supplier s ON ss.SupplierID = s.SupplierID
        JOIN SupplyType st ON ss.SupplyTypeID = st.SupplyTypeID;
    """)
    for row in cursor.fetchall():
        print(row)

    print("\n--- Wine Orders with Distributor and Wine Info (INNER JOIN) ---")
    cursor.execute("""
        SELECT wo.OrderID, d.Name AS Distributor, w.Name AS Wine, wo.Quantity, wo.OrderStatus
        FROM WineOrders wo
        INNER JOIN Distributor d ON wo.DistributorID = d.DistributorID
        INNER JOIN Wine w ON wo.WineID = w.WineID;
    """)
    for row in cursor.fetchall():
        print(row)

    print("\n--- Total Wine Ordered Per Distributor (JOIN with GROUP BY) ---")
    cursor.execute("""
        SELECT d.Name, SUM(wo.Quantity) AS TotalOrdered
        FROM WineOrders wo
        JOIN Distributor d ON wo.DistributorID = d.DistributorID
        GROUP BY d.Name;
    """)
    for row in cursor.fetchall():
        print(row)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    view_joined_data()