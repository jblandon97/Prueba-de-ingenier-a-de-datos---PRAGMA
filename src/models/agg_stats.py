# Modelo y repositorio para la tabla agg_stats
from sqlalchemy import Table, Column, String, BigInteger, Numeric, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
import datetime
import math
from sqlalchemy.sql import text

class AggStatsRepository:
    """
    Repositorio para la tabla 'agg_stats'. Permite realizar upserts de estadísticas acumuladas por batch.
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
            'agg_stats', self.metadata,
            Column('load_batch', String, primary_key=True),
            Column('cum_count', BigInteger),
            Column('cum_avg', Numeric),
            Column('cum_min', Numeric),
            Column('cum_max', Numeric),
            Column('last_updated', String),
        )
        self.metadata.create_all(self.engine)

    def is_invalid(self, value):
        """
        Verifica si un valor es NaN (no válido).
        :param value: Valor a verificar.
        :return: True si es NaN, False en caso contrario.
        """
        return math.isnan(value)

    def upsert_stats(self, batch_name: str, batch_count: int, batch_avg, batch_min, batch_max):
        """
        Inserta o actualiza las estadísticas acumuladas para un batch específico.
        :param batch_name: Nombre del batch.
        :param batch_count: Cantidad de filas en el batch.
        :param batch_avg: Promedio de price en el batch.
        :param batch_min: Mínimo de price en el batch.
        :param batch_max: Máximo de price en el batch.
        :return: Tuple (cum_count, cum_avg, cum_min, cum_max) tras la actualización.
        """
        try:
            batch_count = int(batch_count)
            batch_avg = float(batch_avg)
            batch_min = float(batch_min)
            batch_max = float(batch_max)
            with self.engine.begin() as conn:
                prev = conn.execute(text(
                    "SELECT cum_count, cum_avg, cum_min, cum_max FROM agg_stats ORDER BY last_updated DESC LIMIT 1"
                )).fetchone()
                if prev is None:
                    prev_count, prev_avg, prev_min, prev_max = 0, 0.0, batch_min, batch_max
                else:
                    prev_count, prev_avg, prev_min, prev_max = prev
                if self.is_invalid(batch_avg): batch_avg = prev_avg
                if self.is_invalid(batch_count): batch_count = prev_count
                if self.is_invalid(batch_min): batch_min = prev_min
                if self.is_invalid(batch_max): batch_max = prev_max
                new_count = prev_count + batch_count
                new_avg = ((prev_count * float(prev_avg)) + float(batch_count * batch_avg)) / new_count
                new_min = min(prev_min, batch_min)
                new_max = max(prev_max, batch_max)
                stmt = pg_insert(self.table).values(
                    load_batch=batch_name,
                    cum_count=new_count,
                    cum_avg=new_avg,
                    cum_min=new_min,
                    cum_max=new_max,
                    last_updated=datetime.datetime.utcnow().isoformat()
                )
                do_update = stmt.on_conflict_do_update(
                    index_elements=['load_batch'],
                    set_={
                        'cum_count': new_count,
                        'cum_avg': new_avg,
                        'cum_min': new_min,
                        'cum_max': new_max,
                        'last_updated': datetime.datetime.utcnow().isoformat()
                    }
                )
                conn.execute(do_update)
            return new_count, new_avg, new_min, new_max
        except Exception as e:
            print(f"[ERROR] upsert_stats en agg_stats: {e}")
            return None

    def get_final_stats(self):
        """
        Obtiene el resumen global de estadísticas (count, avg, min, max) desde la tabla 'agg_stats'.
        :return: Tuple (cum_count, cum_avg, cum_min, cum_max)
        """
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text(
                    "SELECT cum_count, cum_avg, cum_min, cum_max FROM agg_stats ORDER BY last_updated DESC LIMIT 1"
                ))
                tot, avgp, minp, maxp = res.fetchone()
            print("== Resumen de las estadísticas ==")
            print(f"Total filas: {tot}")
            print(f"Promedio price: {avgp:.2f}")
            print(f"Mínimo price: {minp}")
            print(f"Máximo price: {maxp}\n")
        except Exception as e:
            print(f"[ERROR] get_final_stats en agg_stats: {e}")
