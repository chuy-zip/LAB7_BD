import os 
import psycopg2
import pandas as pd
from psycopg2 import OperationalError
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy import types as sa_types 
from bson import ObjectId

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
    
def combine_tourism_collections(db):
    collections = [
        'costos_turisticos_africa',
        'costos_turisticos_america',
        'costos_turisticos_asia',
        'costos_turisticos_europa'
    ]
    
    combined_data = []
    
    for collection in collections:
        docs = db[collection].find({})
        combined_data.extend(list(docs))
    
    return pd.DataFrame(combined_data)

def connect_to_mongodb():
    try:
        uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB_NAME")
        
        client = MongoClient( uri )
        db = client[db_name] # nombre de bd de mongo
        print("Conexión exitosa a MongoDB")
        return db
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        return None
    
def get_dataframe_from_table(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error al cargar {table_name}: {e}")
        return None

def save_to_data_warehouse(df, table_name):

    if '_id' in df.columns:
        df['_id'] = df['_id'].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)
    try:
        # credenciales para wl data warehouse
        db_config = {
            'dbname': os.getenv("DW_NAME"),  
            'user': os.getenv("DW_USER"),  
            'password': os.getenv("DW_PASS"),  
            'host': 'localhost',
            'port': os.getenv("DW_PORT")
        }
        # conn con SQLAlchemy 
        engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        # guardar el df
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  
            index=False,
            method='multi',  
            chunksize=1000  
        )
        print(f"\nDataFrame guardado exitosamente en {db_config['dbname']}.{table_name}")
        return True
    except Exception as e:
        print(f"\nError al guardar en data warehouse: {str(e)}")
        print("\nTipos de datos en el DataFrame:")
        print(df.dtypes)
        return False

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

# ahora la parte de mongoDB

mongo_db = connect_to_mongodb()

if mongo_db != None:
    df_costo_globales = combine_tourism_collections(mongo_db)

    print(df_costo_globales.head())
    print(f"Total de países combinados: {len(df_costo_globales)}")

    # esto lo hice porque una de las "columnas" del documento es un diccionario (otro json), entonces hay que procesarlo para que
    # # cad auno sea una columna
    costos_expanded = pd.json_normalize(df_costo_globales['costos_diarios_estimados_en_dólares'])
    print(costos_expanded)

    costos_expanded.columns = [col.replace('.', '_') for col in costos_expanded.columns]

    # Combinar con los datos principales
    df_costos_normalized = pd.concat([df_costo_globales.drop('costos_diarios_estimados_en_dólares', axis=1), costos_expanded], axis=1)
    print("\nColumnas normalizadas:")
    print(df_costos_normalized.columns)
    print(df_costos_normalized.shape)

    big_mac_data = list(mongo_db['paises_mundo_big_mac'].find({}))
    df_big_mac = pd.DataFrame(big_mac_data).drop("_id", axis=1, errors='ignore')

    df_costos_normalized['pais_clean'] = df_costos_normalized['país'].str.lower().str.strip()
    df_big_mac['pais_clean'] = df_big_mac['país'].str.lower().str.strip()

    df_mongo_completo = pd.merge(
        df_costos_normalized,
        df_big_mac[['pais_clean', 'precio_big_mac_usd']],
        on='pais_clean',
        how='left'
    )

    df_mongo_completo = df_mongo_completo.drop(columns=['pais_clean'])

    print("\n Luego de agregar big mac columna:")
    print(df_mongo_completo.columns)
    print(df_mongo_completo.shape)


# combinacion de postgres y mongo
if connection is not None and mongo_db is not None and not df_postgre_completo.empty and not df_mongo_completo.empty:
    df_postgre_completo['pais_clean'] = df_postgre_completo['pais'].str.lower().str.strip()
    df_mongo_completo['pais_clean'] = df_mongo_completo['país'].str.lower().str.strip()

    #Realmente la unica columna que no tiene el set de datos de mongo es la columna de tasa de envejecimiento.
    # aparte de esta columna, todas las columnas de postgres ya se incluyen dentro de los datos de mongo.
    # columnas como la capital, población y todas las variaciones de los costos/precios diarios 
    df_final = pd.merge(
        df_mongo_completo,
        df_postgre_completo[['pais_clean', 'tasa_de_envejecimiento']],
        on='pais_clean',
        how='left'
    )

    df_final = df_final.drop(columns=['pais_clean'])

    print("\nColumnas finales luego del merge con postgres y mongo")
    print(df_final.columns)
    print(df_final.shape)

    valores_nulos_por_columna = df_final.isna().sum()
    print(valores_nulos_por_columna)

    df_final.to_csv('paises_turisticos_data_warehouse.csv', index=False, encoding='utf-8-sig')

    if not df_final.empty:
        succes = save_to_data_warehouse(df_final, 'paises_turisticos')

        if succes:
            print("Se ha guardado la información en el DW")
        else: 
            print("No se ha guardado la información en el DW")
    
    else:
        print("El df_final se encontraba vacío")