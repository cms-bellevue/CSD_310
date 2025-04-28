'''
Clint Scott
CSD310 Module 8.2 Assignment: Movies: Updates & Deletes
4/27/25
'''

import mysql.connector
from dotenv import dotenv_values
from colorama import Fore, Style, init

init()

# Load database credentials from .env file
secrets = dotenv_values(".env")

# Database connection configuration
db_config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
}

def show_films(cursor, title):
    """
    Retrieve and display film data, including Name, Director, Genre, and Studio Name,
    by joining the film, genre, and studio tables.

    Args:
        cursor: Active MySQL cursor used to execute database queries.
        title: A string title displayed above the list of films.
    """
    print(f"\n {Fore.CYAN}-- {title.upper()} --{Style.RESET_ALL}")
    query = """
    SELECT
        f.film_name AS Name,
        f.film_director AS Director,
        g.genre_name AS Genre,
        s.studio_name AS 'Studio Name'
    FROM film f
    INNER JOIN genre g ON f.genre_id = g.genre_id
    INNER JOIN studio s ON f.studio_id = s.studio_id
    """
    cursor.execute(query)
    films = cursor.fetchall()
    for film in films:
        print(f"Film Name: {Fore.GREEN}{film[0]}{Style.RESET_ALL}")
        print(f"Director: {Fore.YELLOW}{film[1]}{Style.RESET_ALL}")
        print(f"Genre Name ID: {Fore.BLUE}{film[2]}{Style.RESET_ALL}")
        print(f"Studio Name: {Fore.MAGENTA}{film[3]}{Style.RESET_ALL}\n")

try:
    # Establish connection to the MySQL database
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()

    # Display initial list of films
    show_films(cursor, "DISPLAYING FILMS")

    # Insert a new studio record for Warner Bros. Pictures
    insert_studio_query = "INSERT INTO studio (studio_name) VALUES ('Warner Bros. Pictures')"
    cursor.execute(insert_studio_query)
    db.commit()
    warner_bros_studio_id = cursor.lastrowid  # Capture the newly inserted studio's ID
    print(f"{Fore.GREEN}Inserted studio: Warner Bros. Pictures with ID {warner_bros_studio_id}{Style.RESET_ALL}")

    # Insert a new film record for 'Inception' associated with Warner Bros. Pictures and SciFi genre (genre_id=2)
    insert_query = f"""
    INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, genre_id, studio_id)
    VALUES ('Inception', '2010', 148, 'Christopher Nolan', 2, {warner_bros_studio_id})
    """
    cursor.execute(insert_query)
    db.commit()
    print(f"{Fore.GREEN}Inserted new film: Inception{Style.RESET_ALL}")

    # Display updated list of films after insertion
    show_films(cursor, "FILMS AFTER INSERTION")

    # Update the genre of 'Alien' to Horror (assuming genre_id=1 corresponds to Horror)
    update_query = """
    UPDATE film
    SET genre_id = 1
    WHERE film_name = 'Alien'
    """
    cursor.execute(update_query)
    db.commit()
    print(f"{Fore.GREEN}Updated Alien to Horror genre.{Style.RESET_ALL}")

    # Display updated list of films after genre change
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE")

    # Delete the film 'Gladiator' from the database
    delete_query = """
    DELETE FROM film
    WHERE film_name = 'Gladiator'
    """
    cursor.execute(delete_query)
    db.commit()
    print(f"{Fore.GREEN}Deleted film: Gladiator{Style.RESET_ALL}")

    # Display final list of films after deletion
    show_films(cursor, "DISPLAYING FILMS AFTER DELETION")

except mysql.connector.Error as err:
    # Handle database connection or execution errors
    print(f"Error: {err}")

finally:
    # Ensure database connection is closed properly
    if 'db' in locals() and db.is_connected():
        cursor.close()
        db.close()
        print("\nMySQL connection closed.")