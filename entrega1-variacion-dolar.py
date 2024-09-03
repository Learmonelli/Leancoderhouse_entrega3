import requests
import pandas as pd
import redshift_connector
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv('./env/config.env')

# Leer las credenciales de las variables de entorno
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DB = os.getenv('REDSHIFT_DB')

# URL base de la API
base_url = "https://api.bcra.gob.ar"

# Parámetros de la API
id_variable = 4  # Reemplaza con el ID de la variable para la variación del dólar
desde = "2024-04-01"  # Fecha de inicio (formato: AAAA-MM-DD)
hasta = datetime.now().strftime("%Y-%m-%d")  # Fecha de fin es la fecha actual
table_name = "variacion_dolar"

# Construye la URL completa
endpoint = f"/estadisticas/v2.0/DatosVariable/{id_variable}/{desde}/{hasta}"
url_completa = base_url + endpoint

try:
    response = requests.get(url_completa, verify=False)
    response.raise_for_status()  # Lanza una excepción si la respuesta HTTP es un error
    response_data = response.json()

    # Crea un DataFrame con la respuesta de la API
    df = pd.DataFrame(response_data)

    # Expande la columna 'results' en columnas separadas
    results_expanded = pd.json_normalize(df['results'])
    df_expanded = pd.concat([df.drop(columns=['results']), results_expanded], axis=1)

    # Imprime el DataFrame expandido
    print(df_expanded)

    # Conectar a Redshift usando redshift_connector
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        port=int(REDSHIFT_PORT)  # Convertir el puerto a entero
    )

    # Crear un cursor
    cursor = conn.cursor()

    # Crear la tabla en Redshift
    create_table_query = """
    CREATE TABLE IF NOT EXISTS variacion_dolar (
        idVariable INT,
        fecha DATE,
        valor DECIMAL(18, 2)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insertar datos en la tabla
    for index, row in df_expanded.iterrows():
        insert_query = """
        INSERT INTO variacion_dolar (idVariable, fecha, valor) VALUES (%s, %s, %s);
        """
        cursor.execute(insert_query, (row['idVariable'], row['fecha'], row['valor']))
    
    conn.commit()

    print("Datos cargados en la tabla variacion_dolar en Redshift con éxito!")

except Exception as e:
    print("Unable to connect to Redshift or execute query.")
    print(e)

finally:
    if conn:
        conn.close()
