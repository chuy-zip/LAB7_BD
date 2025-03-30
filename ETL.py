import os 
import psycopg2
import pandas as pd
from psycopg2 import OperationalError
from dotenv import load_dotenv


def create_connection():
    try:
        conn = psycopg2.connect(
            database=os.getenv("DB"),
            user=os.getenv("USER"),
            password=os.getenv("PASS"),
            host="localhost",
            port=os.getenv("PORT")
        )
        return conn
    except OperationalError as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None

def get_dataframe_from_table(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error al cargar {table_name}: {e}")
        return None

load_dotenv()

connection = create_connection()
if connection:
    df_poblacion = get_dataframe_from_table(connection, "pais_poblacion")
    df_envejecimiento = get_dataframe_from_table(connection, "pais_envejecimiento")
    
    # procesando el nombre antes de hacer el join, sino puede dar errores
    df_poblacion['pais_clean'] = df_poblacion['pais'].str.strip().str.lower()
    df_envejecimiento['nombre_pais_clean'] = df_envejecimiento['nombre_pais'].str.strip().str.lower()

    print("\nDataFrame Población:")
    print(df_poblacion.head())
    print("\nDataFrame Envejecimiento:")
    print(df_envejecimiento.head())

    df_postgre_completo = pd.merge(
        df_poblacion,
        df_envejecimiento[['nombre_pais_clean', 'tasa_de_envejecimiento']],  # Solo traer la columna que hace falta
        left_on='pais_clean',
        right_on='nombre_pais_clean',
        how='left'
    )

    # remov columnas que ya no son necesarios luego del join
    df_postgre_completo = df_postgre_completo.drop("_id", axis=1)
    df_postgre_completo = df_postgre_completo.drop(columns=['pais_clean', 'nombre_pais_clean'])

    print("\nDataFrame postgres final:")
    print(df_postgre_completo.head())
    print(df_postgre_completo.shape)

    connection.close()


