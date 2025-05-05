"""
Author: Jelani Jenkins & Clint Scott
Date: 05/04/2025
Assignment: Module 10.1 - Create Table Script

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

def create_tables():
    conn = reconnect_to_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Department (
                DepartmentID INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255) NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Employee (
                EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255) NOT NULL,
                DepartmentID INT,
                Position VARCHAR(255),
                FOREIGN KEY (DepartmentID) REFERENCES Department(DepartmentID)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS EmployeeHours (
                RecordID INT AUTO_INCREMENT PRIMARY KEY,
                EmployeeID INT,
                Week DATE,
                HoursWorked DECIMAL(5,2),
                FOREIGN KEY (EmployeeID) REFERENCES Employee(EmployeeID)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SupplyType (
                SupplyTypeID INT AUTO_INCREMENT PRIMARY KEY,
                Description VARCHAR(255)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Supplier (
                SupplierID INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SupplyShipment (
                ShipmentID INT AUTO_INCREMENT PRIMARY KEY,
                SupplierID INT,
                SupplyTypeID INT,
                Quantity INT,
                ExpectedDeliveryDate DATE,
                ActualDeliveryDate DATE,
                FOREIGN KEY (SupplierID) REFERENCES Supplier(SupplierID),
                FOREIGN KEY (SupplyTypeID) REFERENCES SupplyType(SupplyTypeID)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SupplyInventory (
                SupplyInventoryID INT AUTO_INCREMENT PRIMARY KEY,
                SupplyTypeID INT,
                QuantityOnHand INT,
                LastUpdated TIMESTAMP,
                FOREIGN KEY (SupplyTypeID) REFERENCES SupplyType(SupplyTypeID)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Wine (
                WineID INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255),
                Type VARCHAR(255)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Distributor (
                DistributorID INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WineOrders (
                OrderID INT AUTO_INCREMENT PRIMARY KEY,
                DistributorID INT,
                WineID INT,
                Quantity INT,
                OrderDate DATE,
                ShipDate DATE,
                OrderStatus VARCHAR(100),
                FOREIGN KEY (DistributorID) REFERENCES Distributor(DistributorID),
                FOREIGN KEY (WineID) REFERENCES Wine(WineID)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WineInventory (
                WineInventoryID INT AUTO_INCREMENT PRIMARY KEY,
                WineID INT,
                QuantityOnHand INT,
                LastUpdated TIMESTAMP,
                FOREIGN KEY (WineID) REFERENCES Wine(WineID)
            );
        """)

        conn.commit()
        print("All tables created successfully.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables()