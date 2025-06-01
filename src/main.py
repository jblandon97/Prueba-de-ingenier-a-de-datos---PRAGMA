# Script principal del pipeline de datos para la prueba PRAGMA SAS

"""
Este archivo ejecuta el pipeline solicitado en la prueba de ingeniería de datos de PRAGMA SAS.
- Procesa los archivos CSV de la carpeta `data/` uno por uno (nunca todos en memoria).
- Inserta los datos en la base de datos y actualiza estadísticas acumuladas tras cada fila y lote.
- Imprime la evolución de las estadísticas en consola y el resumen antes y después de cargar `validation.csv`.
- El código está comentado paso a paso para facilitar su comprensión.

Modo de uso:
- Para procesar los archivos principales (sin validation.csv):
    python src/main.py
- Para procesar solo validation.csv (en un segundo momento):
    python src/main.py --ejecutar-validation
"""

import os
import sys
from config.db import engine, metadata
from models.transactions import TransactionsRepository
from models.agg_stats import AggStatsRepository
from models.ingested_files import IngestedFilesRepository
from etl.etl_pipeline import ETLPipeline
from sqlalchemy import text

sys.path.append(os.path.abspath(os.path.dirname(__file__)))  # Asegura que los imports funcionen correctamente

data_dir = os.path.join(os.path.dirname(__file__), "../data")  # Carpeta donde están los CSV

# --- Configuración de acceso a las tablas principales ---
# Se inicializan las clases encargadas de interactuar con cada tabla de la base de datos.
# Cada clase implementa los métodos para insertar, consultar y actualizar datos en su respectiva tabla.
transactions_table = TransactionsRepository(engine, metadata)   # transactions: Maneja la tabla de transacciones (eventos individuales)
agg_stats_table = AggStatsRepository(engine, metadata)          # agg_stats: Maneja la tabla de estadísticas acumuladas por lote
ingested_files_table = IngestedFilesRepository(engine, metadata)  # ingested_files: Maneja la tabla de control de batches procesados

# Se crea el pipeline ETL, que coordina la extracción, transformación, carga y actualización de estadísticas.
# El pipeline recibe las clases de acceso a las tablas para operar sobre los datos de manera modular.
etl_pipeline = ETLPipeline(transactions_table, agg_stats_table, ingested_files_table)

# Se obtiene el listado de batches ya procesados para evitar reprocesar archivos.
processed = ingested_files_table.get_processed_batches()
print(f"Batches ya procesados: {processed}\n")

# Esquema de la tabla transactions (para transformar tipos de datos)
transactions_schema = {
    'user_id': int,   # user_id debe ser entero
    'price': float    # price debe ser float
}

try:
    # --- Procesamiento de archivos CSV ---
    if '--ejecutar-validation' in sys.argv:
        # Solo ejecuta validation.csv
        if "validation" in processed:
            print("⚠️  'validation' ya procesado, omitiendo.")
        else:
            print("=== Ejecutando validation.csv ===")
            fname = "validation.csv"
            path = os.path.join(data_dir, fname)
            # Procesa validation.csv igual que los otros, mostrando estadísticas por fila
            etl_pipeline.process_file(path, "validation", transactions_schema, print_stats_per_row=True)
            print("✅  'validation' procesado correctamente.")
            ingested_files_table.mark_batch_loaded("validation")
        # Imprime estadísticas finales tras validation.csv
        print("=== Estadísticas después de validation.csv ===")
        agg_stats_table.get_final_stats()
    else:
        # Procesa todos los archivos excepto validation.csv
        for fname in sorted(os.listdir(data_dir)):
            if not fname.endswith(".csv") or fname == "validation.csv":
                continue  # Solo procesa archivos CSV distintos de validation.csv
            batch = fname.replace(".csv", "")  # Nombre del batch (ej: '2012-1')
            if batch in processed:
                print(f"⚠️  '{batch}' ya procesado, omitiendo.")
                continue
            path = os.path.join(data_dir, fname)  # Ruta completa al archivo CSV
            # Procesa el archivo: inserta filas y actualiza estadísticas por fila
            etl_pipeline.process_file(path, batch, transactions_schema, print_stats_per_row=True)
            ingested_files_table.mark_batch_loaded(batch)  # Marca el batch como procesado
        # --- Estadísticas antes de validation.csv ---
        print("=== Estadísticas antes de validation.csv ===")
        agg_stats_table.get_final_stats()  # Imprime el resumen acumulado hasta aquí
except Exception as e:
    # Manejo global de errores críticos
    print(f"[ERROR] Error crítico en la ejecución del pipeline: {e}")