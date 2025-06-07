import os
import redshift_connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class ChargerController:
    def __init__(self):
        self.connection = redshift_connector.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT", 5439)),
            database=os.getenv("REDSHIFT_DBNAME"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD")
        )
        self.connection.autocommit = True  # Enable auto-commit mode

    def get_chargers(self, filters=None):
        """
        GET /chargers
        Returns all chargers or filters based on charger_id and city.
        """
        sql = "SELECT * FROM chargers"
        conditions = []
        params = []

        if filters:
            if "charger_id" in filters:
                conditions.append("charger_id = %s")
                params.append(filters["charger_id"])
            if "city" in filters:
                conditions.append("city ILIKE %s")
                params.append(f"%{filters['city']}%")

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        print(f"Executing SQL: {sql} with params: {params}")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                result = cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error executing query in get_chargers: {e}")
            raise

    def get_usage_analytics(self, charger_id, filters=None):
        """
        GET /chargers/{charger_id}/usage-analytics
        Returns usage analytics for a specific charger.
        """
        sql = """
        SELECT COUNT(*) AS total_transactions,
               SUM(kwh_consumed) AS total_kWh,
               MAX(kwh_consumed) AS biggest_transaction_kWh,
               MIN(kwh_consumed) AS smallest_transaction_kWh,
               AVG(kwh_consumed) AS average_transaction_kWh,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY kwh_consumed) AS median_transaction_kWh
        FROM transactions
        WHERE charger_id = %s
        """
        params = [charger_id]

        if filters:
            if "start_datetime" in filters:
                sql += " AND start_time >= %s"
                params.append(filters["start_datetime"])
            if "end_datetime" in filters:
                sql += " AND end_time <= %s"
                params.append(filters["end_datetime"])
            if "status" in filters:
                sql += " AND status = %s"
                params.append(filters["status"])

        print(f"Executing SQL: {sql} with params: {params}")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                result = cursor.fetchone()
            return result
        except Exception as e:
            print(f"Error executing query in get_usage_analytics: {e}")
            raise