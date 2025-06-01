# Configuración de la base de datos y utilidades de conexión
import os
from sqlalchemy import create_engine, MetaData
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
env_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(env_path)

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL, echo=False)
metadata = MetaData()
