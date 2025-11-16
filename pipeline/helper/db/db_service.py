import json
from decimal import Decimal

from psycopg2 import extensions as PsycopgExtension

class DbService:
    @staticmethod
    def fetch_data(cursor: PsycopgExtension.cursor, query_str: str) -> list:
        """
        Params:
            cursor (PsycopgCursor): psycopg2 cursor
            query_str (str): query to execute

        Returns:
            list: list of fetched object
        """

        response_to_return = []

        cursor.execute(query_str)
        response_rows = cursor.fetchall()
        response_columns = [desc[0] for desc in cursor.description]

        if (response_rows):
            for row in response_rows:
                response_dict = {}

                for i, datum in enumerate(row):
                    response_dict[response_columns[i]] = datum
                
                response_to_return.append(response_dict)
        return json.dumps(
            response_to_return, 
            default=lambda o: float(o) if isinstance(o, Decimal) else str(o)
        )