# Modelo y repositorio para la tabla transactions
import pandas as pd
from sqlalchemy import Table, Column, Integer, Numeric, DateTime, MetaData

class TransactionsRepository:
    """
    Repositorio para la tabla 'transactions'. Permite insertar filas y realizar operaciones de carga masiva.
    """
    def __init__(self, engine, metadata=None):
        """
        Inicializa el repositorio y crea la tabla si no existe.
        :param engine: SQLAlchemy engine para la conexión a la base de datos.
        :param metadata: SQLAlchemy MetaData opcional.
        """
        self.engine = engine
        self.metadata = metadata or MetaData()
        self.table = Table(
            "transactions",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("timestamp", DateTime),
            Column("user_id", Integer),
            Column("price", Numeric),
        )
        self.metadata.create_all(self.engine)

    def insert_row(self, row_dict):
        """
        Inserta una fila en la tabla 'transactions'.
        :param row_dict: Diccionario con los datos de la fila.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(self.table.insert().values(**row_dict))
        except Exception as e:
            print(f"[ERROR] insert_row en transactions: {e}")

    def bulk_insert(self, df: pd.DataFrame):
        """
        Inserta múltiples filas en la tabla 'transactions' usando un DataFrame.
        :param df: DataFrame de pandas con los datos a insertar.
        """
        try:
            df.to_sql(self.table.name, self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        except Exception as e:
            print(f"[ERROR] bulk_insert en transactions: {e}")
