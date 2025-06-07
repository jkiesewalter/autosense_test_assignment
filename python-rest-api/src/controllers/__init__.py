import os
import redshift_connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class UserController:
    def __init__(self):
        # Use environment variables for Redshift credentials
        self.connection = redshift_connector.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT", 5439)),
            database=os.getenv("REDSHIFT_DBNAME"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD")
        )

    def create_user(self, user_data):
        sql = "INSERT INTO users (id, name, email) VALUES (%s, %s, %s)"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (user_data['id'], user_data['name'], user_data['email']))
        self.connection.commit()
        return {"message": "User created successfully"}

    def get_user(self, user_id):
        sql = "SELECT * FROM users WHERE id = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
        return result if result else {"message": "User not found"}

    def delete_user(self, user_id):
        sql = "DELETE FROM users WHERE id = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (user_id,))
        self.connection.commit()
        return {"message": "User deleted successfully"}