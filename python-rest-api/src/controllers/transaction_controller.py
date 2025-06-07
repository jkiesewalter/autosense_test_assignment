import os
import redshift_connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class TransactionController:
    def __init__(self):
        self.connection = redshift_connector.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT", 5439)),
            database=os.getenv("REDSHIFT_DBNAME"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD")
        )
        self.connection.autocommit = True  # Enable auto-commit mode

    def get_transactions_extended(self, filters=None):
        """
        GET /transactions-extended
        Returns all transactions in extended format with optional filters.
        """
        sql = "SELECT * FROM transactions"
        conditions = []
        params = []

        if filters:
            if "min_kwh" in filters:
                conditions.append("kWh_consumed >= %s")
                params.append(filters["min_kwh"])
            if "max_kwh" in filters:
                conditions.append("kWh_consumed <= %s")
                params.append(filters["max_kwh"])
            if "min_amount_charged" in filters:
                conditions.append("amount >= %s")
                params.append(filters["min_amount_charged"])
            if "max_amount_charged" in filters:
                conditions.append("amount <= %s")
                params.append(filters["max_amount_charged"])
            if "user_id" in filters:
                conditions.append("user_id = %s")
                params.append(filters["user_id"])
            if "charger_id" in filters:
                conditions.append("charger_id = %s")
                params.append(filters["charger_id"])
            if "start_datetime" in filters:
                conditions.append("start_time >= %s")
                params.append(filters["start_datetime"])
            if "end_datetime" in filters:
                conditions.append("end_time <= %s")
                params.append(filters["end_datetime"])

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        print(f"Executing SQL: {sql} with params: {params}")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                result = cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error executing query in get_transactions_extended: {e}")
            raise