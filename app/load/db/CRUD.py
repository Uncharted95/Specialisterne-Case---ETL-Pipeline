from app.load.db.connection import Connector
from app.load.error_handling.type_control import test_parameter, test_parameters
from app.load.schemas.table_schema import TABLES
from psycopg2 import sql
from app.load.schemas.database_schema import database_schema


class CRUD:
    def __init__(self):
        self.db = Connector(database_schema["database"], database_schema["user"], database_schema["password"], database_schema["host"])


    #NB: The create method currently doesn't work as is
    def create_row(self, table_name: str, row: dict, commit:bool = True, close:bool = True):
        """This method handles creating new rows in tables of the database.
        row must be a dictionary, with keys being column names and values being, well... values.
        The close argument decides whether to close the connection after running the method. By default, it is true
        The commit argument decides whether a change should be commited to the database immediately. By default, it is true."""
        columns = TABLES.get(table_name)
        if columns is None:
            raise ValueError(f"Unknown table: {table_name}")
        columns = list(columns.keys())
        #Check that all required columns are there
        missing = [col for col in columns if col not in row]
        if missing:
            raise ValueError(f"Missing columns for table '{table_name}': {missing}")

        #Building the query
        placeholders = ", ".join([f"%({col})s" for col in columns])

        column_names = [sql.Identifier(col_name) for col_name in columns]

        #Building the query
        query = sql.SQL("""INSERT INTO {} ({})""").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(column_names)
        )
        query +="""\n
        VALUES ({placeholders})
        """

        self.db.execute(query, row, commit=commit, close=close)


    def create_mult_rows(self,table_name:str, rows: list[dict], commit:bool = True, close:bool = True):
        """This method handles creating multiple new rows in a designated table of the database.
        rows must be a list of dictionaries, with keys being column names and values being, well... values.
        The close argument decides whether to close the connection after running the method. By default, it is true
        The commit argument decides whether a change should be commited to the database immediately. By default, it is true."""
        columns = TABLES.get(table_name)
        if columns is None:
            raise ValueError(f"Unknown table: {table_name}")
        columns = list(columns.keys())

        #Check that all required columns are there
        for i, row in enumerate(rows):
            missing = [col for col in columns if col not in row]
            if missing:
                raise ValueError(f"Row {i} is missing columns for table '{table_name}': {missing}")

        column_names = [sql.Identifier(col_name) for col_name in columns]
        values = [[row[col] for col in columns] for row in rows]
        #Building the query
        query = sql.SQL("""INSERT INTO {} ({})
        VALUES %s
        """).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(column_names)
        )
        self.db.execute_mult(query, values, commit=commit, close=close)

    def delete_all_rows(self, table_name:str):
        """This method deletes all rows of the given table.
        It is a nuclear option and should be handled with care."""
        query = sql.SQL("TRUNCATE TABLE {}").format(
        sql.Identifier(table_name)
        )
        print(f"Deleting all rows from table {table_name}")
        self.db.execute(query, commit=True)

    def cleanse_db(self):
        """This method deletes every single row in every single table of the database.
        It is a nuclear option and should be handled with care.
        Note that it does not reset the indices of the tables"""
        for table in TABLES:
            self.delete_all_rows(table)

        #The methods below this line need to be modified for the current database
# ---------------------------------------------------------------------------------------

    def read(self,*, data_notes_id: int = None, entity_id: int = None, entity_type: str = None, limit: int = None, is_df: bool = False):
        """This method handles reading rows from the data_notes table.
        If no row id, entity id, entity type or limit is given, the function reads all rows.
        At present, it cannot filter by time."""
        sql = "SELECT * FROM data_notes"
        conditions = []
        parameters = {}
        test_parameters([data_notes_id, entity_id, entity_type],[int,int,str])
        if data_notes_id is not None:
            conditions.append("data_notes_id = %(data_notes_id)s")
            parameters["data_notes_id"] = data_notes_id

        if entity_id is not None:
            conditions.append("entity_id = %(entity_id)s")
            parameters["entity_id"] = entity_id

        if entity_type is not None:
            conditions.append("entity_type = %(entity_type)s")
            parameters["entity_type"] = entity_type

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        sql += " ORDER BY created_at DESC"

        if limit is not None:
            sql +="\n LIMIT %(limit)s"
            parameters["limit"] = limit

        self.display_results(sql, parameters, is_df)

    def display_results(self, sql, parameters, is_df=False):
        self.db.connect()
        if is_df:
            print(self.db.query_as_df(sql, parameters))
        else:
            print(("data_notes_id", "data_notes_id", "entity_id", "entity_type", "created_at"))
            rows = self.db.query(sql, parameters)
            for row in rows:
                print(row)
        self.db.close()




    def update(self, *, data_notes_id: int, entity_id: int= None, entity_type: str = None, comment: str= None):
        """This method handles updates to the data_notes table. """
        sql = """UPDATE data_notes
        SET """
        updates = []
        parameters = {}
        test_parameters([data_notes_id, entity_id, entity_type, comment],[int,int,str,str])
        if entity_id is not None:
            updates.append("entity_id = %(entity_id)s")
            parameters["entity_type"] = entity_id

        if entity_type is not None:
            updates.append("entity_type = %(entity_type)s")
            parameters["entity_type"] = entity_type

        if comment is not None:
            updates.append("comment = %(comment)s")
            parameters["comment"] = comment

        if updates:
            sql += ", ".join(updates)

        sql += "\nWHERE data_notes_id = %(data_notes_id)s"
        parameters["data_notes_id"] = data_notes_id

        self.db.execute(sql, parameters)

    def delete(self,data_notes_id: int):
        """This method deletes a single row from the data_notes table based on the unique data_notes_id"""
        sql = """DELETE FROM data_notes
        WHERE data_notes_id = %(data_notes_id)s"""
        test_parameter(data_notes_id, int)
        parameters = {"data_notes_id": data_notes_id}
        self.db.execute(sql, parameters, commit = True)

