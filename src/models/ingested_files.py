# Modelo y repositorio para la tabla ingested_files
from sqlalchemy import Table, Column, String, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
import datetime

class IngestedFilesRepository:
    """
    Repositorio para la tabla 'ingested_files'. Permite registrar y consultar los batches ya procesados.
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
            'ingested_files', self.metadata,
            Column('batch_name', String, primary_key=True),
            Column('loaded_at', String, default=lambda: datetime.datetime.utcnow().isoformat())
        )
        self.metadata.create_all(self.engine)

    def get_processed_batches(self):
        """
        Obtiene el conjunto de nombres de batches ya procesados.
        :return: Set de nombres de batches.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(self.table.select())
                return {row[0] for row in result.fetchall()}
        except Exception as e:
            print(f"[ERROR] get_processed_batches en ingested_files: {e}")
            return set()

    def mark_batch_loaded(self, batch_name):
        """
        Marca un batch como procesado en la tabla 'ingested_files'.
        :param batch_name: Nombre del batch a registrar.
        """
        try:
            with self.engine.connect() as conn:
                stmt = pg_insert(self.table).values(batch_name=batch_name)
                conn.execute(stmt.on_conflict_do_nothing(index_elements=['batch_name']))
                conn.commit()
        except Exception as e:
            print(f"[ERROR] mark_batch_loaded en ingested_files: {e}")
