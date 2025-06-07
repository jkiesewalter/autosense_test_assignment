import os
import re
import redshift_connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class UserController:
    def __init__(self):
        self.connection = redshift_connector.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT", 5439)),
            database=os.getenv("REDSHIFT_DBNAME"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD")
        )
        self.connection.autocommit = True  # Enable auto-commit mode

    def get_users(self, filters=None):
        """
        GET /users
        Returns all users or filters based on user_id, first_name, last_name, and email.
        """
        sql = """
        SELECT * FROM users
        """
        conditions = []
        params = []

        if filters:
            if "user_id" in filters:
                conditions.append("user_id = %s")
                params.append(filters["user_id"])
            if "first_name" in filters:
                conditions.append("LOWER(first_name) ILIKE %s")
                params.append(f"%{filters['first_name'].lower()}%")
            if "last_name" in filters:
                conditions.append("LOWER(last_name) ILIKE %s")
                params.append(f"%{filters['last_name'].lower()}%")
            if "email" in filters:
                conditions.append("LOWER(email) ILIKE %s")
                params.append(f"%{filters['email'].lower()}%")

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        print(f"Executing SQL: {sql} with params: {params}")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                result = cursor.fetchall()
                return result
        except Exception as e:
            print(f"Error executing query: {e}")
            raise