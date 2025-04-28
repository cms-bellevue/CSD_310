'''
Clint Scott
CSD310 Module 7.2 Assignment: Movies: Table Queries
4/27/25
'''

import mysql.connector
from dotenv import dotenv_values
from colorama import Fore, Style, init
import os

# Initialize colorama for color support in terminal
init()

# Constants for environment variable keys
USER_KEY = "USER"
PASSWORD_KEY = "PASSWORD"
HOST_KEY = "HOST"
DATABASE_KEY = "DATABASE"

# Constants for database queries
STUDIO_QUERY = "SELECT studio_id, studio_name FROM studio"
GENRE_QUERY = "SELECT genre_id, genre_name FROM genre"
SHORT_FILM_QUERY = "SELECT film_name, film_runtime FROM film WHERE film_runtime < 120"
DIRECTOR_QUERY = """
    SELECT film_name, film_director
    FROM film
    WHERE film_name IN ('Get Out', 'Gladiator', 'Alien')
    ORDER BY FIELD(film_name, 'Get Out', 'Gladiator', 'Alien')
"""

def load_secrets():
    """
    Load database credentials from a .env file.

    Returns:
        dict: A dictionary containing the database credentials (user, password, host, database).
              Exits the program if any required variable is missing.
    """
    secrets = dotenv_values(".env")

    # Check if all required environment variables are present
    required_vars = [USER_KEY, PASSWORD_KEY, HOST_KEY, DATABASE_KEY]
    for var in required_vars:
        if var not in secrets:
            print(f"Error: Missing {var} in .env file")
            exit(1)

    return secrets

def connect_to_database(db_config):
    """
    Establish a connection to the MySQL database.

    Args:
        db_config (dict): A dictionary containing the database connection parameters.

    Returns:
        tuple: A tuple containing the database connection object and the cursor object.
               Exits the program if a connection error occurs.
    """
    try:
        # Connect to the MySQL database using the provided configuration
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor()
        return db, cursor
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)

def print_header(title):
    """
    Prints a formatted header for each section of the output.

    Args:
        title (str): The title of the section to be displayed.
    """
    print(f"\n{Fore.CYAN}-- DISPLAYING {title.upper()} RECORDS --{Style.RESET_ALL}\n")

def fetch_and_display_studios(cursor, query):
    """
    Fetches and displays studio records in the specified format.

    Args:
        cursor (mysql.connector.cursor.MySQLCursor): The database cursor object.
        query (str): The SQL query to execute.
    """
    print_header("Studio")
    cursor.execute(query)
    studio_records = cursor.fetchall()
    for record in studio_records:
        print(f"Studio ID: {Fore.GREEN}{record[0]}{Style.RESET_ALL}")
        print(f"Studio Name: {Fore.YELLOW}{record[1]}{Style.RESET_ALL}")
        print()

def fetch_and_display_genres(cursor, query):
    """
    Fetches and displays genre records in the specified format.

    Args:
        cursor (mysql.connector.cursor.MySQLCursor): The database cursor object.
        query (str): The SQL query to execute.
    """
    print_header("Genre")
    cursor.execute(query)
    genre_records = cursor.fetchall()
    for record in genre_records:
        print(f"Genre ID: {Fore.GREEN}{record[0]}{Style.RESET_ALL}")
        print(f"Genre Name: {Fore.YELLOW}{record[1]}{Style.RESET_ALL}")
        print()

def fetch_and_display_short_films(cursor, query):
    """
    Fetches and displays short film records in the specified format.

    Args:
        cursor (mysql.connector.cursor.MySQLCursor): The database cursor object.
        query (str): The SQL query to execute.
    """
    print_header("Short Film")
    cursor.execute(query)
    short_film_records = cursor.fetchall()
    for record in short_film_records:
        print(f"Film Name: {Fore.GREEN}{record[0]}{Style.RESET_ALL}")
        print(f"Runtime: {Fore.YELLOW}{record[1]}{Style.RESET_ALL}")
        print()

def fetch_and_display_directors(cursor, query):
    """
    Fetches and displays director records in the specified format and order.

    Args:
        cursor (mysql.connector.cursor.MySQLCursor): The database cursor object.
        query (str): The SQL query to execute.
    """
    print(f"\n{Fore.CYAN}-- DISPLAYING DIRECTOR RECORDS IN ORDER --{Style.RESET_ALL}\n")
    cursor.execute(query)
    director_records = cursor.fetchall()
    for record in director_records:
        print(f"Film Name: {Fore.GREEN}{record[0]}{Style.RESET_ALL}")
        print(f"Director: {Fore.YELLOW}{record[1]}{Style.RESET_ALL}")
        print()

def main():
    """
    Main function to load secrets, connect to the database, and display the results of the four queries
    in a format that matches the provided image.
    """
    # Load database credentials
    secrets = load_secrets()

    # Prepare database configuration
    db_config = {
        "user": secrets[USER_KEY],
        "password": secrets[PASSWORD_KEY],
        "host": secrets[HOST_KEY],
        "database": secrets[DATABASE_KEY],
    }

    # Connect to the database
    db, cursor = connect_to_database(db_config)

    try:
        # First Query: Select all fields from the studio table
        fetch_and_display_studios(cursor, STUDIO_QUERY)

        # Second Query: Select all fields from the genre table
        fetch_and_display_genres(cursor, GENRE_QUERY)

        # Third Query: Select movie names and runtime for short films
        fetch_and_display_short_films(cursor, SHORT_FILM_QUERY)

        # Fourth Query: Get film names and directors in the specified order
        fetch_and_display_directors(cursor, DIRECTOR_QUERY)

    except mysql.connector.Error as err:
        print(f"Error while fetching records: {err}")

    finally:
        # Ensure proper cleanup of resources
        if 'db' in locals() and db.is_connected():
            cursor.close()
            db.close()
            print("\nMySQL connection closed.")

# Run the main function when script is executed
if __name__ == "__main__":
    main()