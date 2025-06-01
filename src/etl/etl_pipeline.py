# Clase para orquestar el pipeline ETL
import pandas as pd

class ETLPipeline:
    """
    Orquesta el flujo ETL: extracción, transformación y carga de datos, y actualización de estadísticas.
    """
    def __init__(self, transactions_repo, agg_stats_repo, ingested_files_repo):
        """
        Inicializa el pipeline ETL con los repositorios necesarios.
        :param transactions_repo: Repositorio de transacciones.
        :param agg_stats_repo: Repositorio de estadísticas acumuladas.
        :param ingested_files_repo: Repositorio de batches procesados.
        """
        self.transactions_repo = transactions_repo
        self.agg_stats_repo = agg_stats_repo
        self.ingested_files_repo = ingested_files_repo

    def _extract(self, file_path):
        """
        Extrae los datos de un archivo CSV a un DataFrame.
        :param file_path: Ruta al archivo CSV.
        :return: DataFrame con los datos extraídos.
        """
        return pd.read_csv(file_path, parse_dates=['timestamp'])

    @staticmethod
    def apply_schema(df, schema: dict):
        """
        Aplica un esquema de tipos de datos a un DataFrame.
        :param df: DataFrame de pandas.
        :param schema: Diccionario {columna: tipo}.
        :return: DataFrame con los tipos aplicados.
        """
        for col, dtype in schema.items():
            df[col] = df[col].astype(dtype)
        return df

    def process_file(self, file_path, batch_name, schema, print_stats_per_row=False):
        """
        Procesa un archivo: extrae, transforma según el esquema, inserta filas y actualiza estadísticas.
        :param file_path: Ruta al archivo CSV.
        :param batch_name: Nombre del batch.
        :param schema: Esquema de tipos de datos a aplicar.
        :param print_stats_per_row: Si es True, imprime estadísticas acumuladas por cada fila procesada.
        :return: None
        """
        try:
            df = self._extract(file_path)
        except Exception as e:
            print(f"[ERROR] Fallo al extraer datos de {file_path}: {e}")
            return
        try:
            df = self.apply_schema(df, schema)
        except Exception as e:
            print(f"[ERROR] Fallo al transformar datos de {file_path}: {e}")
            return
        last_stats = None
        for idx, row in df.iterrows():
            try:
                row_dict = row.to_dict()
                self.transactions_repo.insert_row(row_dict)
                last_stats = self.agg_stats_repo.upsert_stats(
                    batch_name=batch_name,
                    batch_count=1,
                    batch_avg=row['price'],
                    batch_min=row['price'],
                    batch_max=row['price']
                )
                if print_stats_per_row and last_stats:
                    cum_count, cum_avg, cum_min, cum_max = last_stats
                    print(
                        f"🟢 date={row['timestamp']} | "
                        f"cum_count={cum_count:,} | "
                        f"cum_avg={cum_avg:.2f} | "
                        f"cum_min={cum_min:.2f} | "
                        f"cum_max={cum_max:.2f}"
                    )
            except Exception as e:
                print(f"[ERROR] Fila {idx} en {file_path} no procesada: {e}")
                continue
        if last_stats:
            cum_count, cum_avg, cum_min, cum_max = last_stats
            print(
                f"🔵 batch={batch_name} | "
                f"cum_count={cum_count:,} | "
                f"cum_avg={cum_avg:.2f} | "
                f"cum_min={cum_min:.2f} | "
                f"cum_max={cum_max:.2f}"
            )
