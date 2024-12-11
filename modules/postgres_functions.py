import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text

def probar_conexion_postgresql(host, user, clave, database, port):
    try:
        conexion = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=clave,
            port=port
        )

        print("Conexión exitosa a PostgreSQL")
        conexion.close()

    except Exception as e:
        print(f"Error al intentar conectar a PostgreSQL: {e}")

def obtener_tablas_disponibles(host, password, database, user, port):
    conexion = None
    try:
        conexion = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = conexion.cursor()
        consulta_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        cursor.execute(consulta_sql)
        tablas_disponibles = [tabla[0] for tabla in cursor.fetchall()]
        return tablas_disponibles

    except Exception as e:
        print(f"Error obtaining tables list: {e}")

    finally:
        if conexion is not None:
            cursor.close()
            conexion.close()

def sql_query(host, password, database, user, port, query, values=None, fetch=True):
    conexion = None
    try:
        conexion = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = conexion.cursor()

        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)

        if fetch:
            return cursor.fetchall()
        else:
            conexion.commit()
            fetch = cursor.fetchall()
            print(f"Query executed successfully: {fetch}")

    except Exception as e:
        print(f"Error executing query: {e}")

    finally:
        if conexion is not None:
            cursor.close()
            conexion.close()

def df_to_sql(host, password, database, user, port, df, nombre, metodo, index_label=None):
    conexion = None
    try:
        conexion = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        df.to_sql(nombre, con=engine, if_exists=metodo, index=False, index_label=index_label)
        print(f"Tabla {nombre} creada exitosamente en la base de datos {database} del servidor {host}")
        
    except Exception as e:
        print(f"Error al enviar la tabla : {e}")

    finally:
        if conexion is not None:
            conexion.close()

def dato_columnas(host, password, database, user, port, tabla_nombre):
    query = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = '{tabla_nombre}';
    """
    tipos = sql_query(host, password, database, user, port, query)
    df_info_columnas = pd.DataFrame(tipos, columns=['columna', 'tipo'])
    return df_info_columnas

def uniformizar_datos(host, password, database, user, port, df_entrada, tabla, arreglo=True):
    df = df_entrada.copy()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.replace('', np.nan)
    df_datos = dato_columnas(host, password, database, user, port, tabla)
    col_tabla = df_datos['columna'].to_list()
    tipo_fechas = ['date', 'timestamp without time zone']
    tipo_int = ['integer', 'bigint', 'smallint']
    tipo_str = ['character', 'character varying', 'text']
    for col in df.columns:
        if col in col_tabla:
            tipo_dato = df_datos.loc[df_datos['columna'] == col, 'tipo'].iloc[0]
            if tipo_dato in tipo_fechas:
                df[col] = df[col].replace(0, np.nan)
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif tipo_dato == 'date':
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce').dt.date
                df[col] = df[col].where(pd.notnull(df[col]), None)
            elif tipo_dato == 'time without time zone':
                df[col] = df[col].replace(0, np.nan)
                df[col] = pd.to_datetime(df[col], format='%H:%M:%S').dt.time
                df[col] = df[col].where(pd.notnull(df[col]), None)
            elif tipo_dato in tipo_int:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            elif tipo_dato == 'double precision':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(np.nan).astype(float)
            elif tipo_dato == 'boolean':
                df[col] = df[col].apply(lambda x: 
                        False if x.strip() not in ['True', 'False', 'SI', 'NO'] 
                        else (True if x.strip() == 'True' or x.strip() == 'SI' else False))
                df[col] = df[col].fillna(False)
                df[col] = df[col].astype(bool)
            elif tipo_dato in tipo_str:
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else np.nan)
        else:
            print('No esta dentro de la tabla : ' + str(col))
    
    if arreglo:
        columnas_presentes = [col for col in col_tabla if col in df.columns]
        df = df[columnas_presentes].copy()
        return df
    else:
        return df.copy()

def sql_to_df(host, password, database, user, port, query):
    conexion = None
    try:
        conexion = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

        with engine.connect() as conn:
          result = conn.execute(text(query))
          columns = result.keys()
          main_df = pd.DataFrame(result.fetchall(), columns=columns)

        return main_df

    except Exception as e:
        print(f"Error : {e}")

    finally:
        if conexion is not None:
            conexion.close()

def create_sql_script(df, table_name, file_name, index_cols=None, pk_name=None, date_cols=True, transaction=None):
    # Lista de caracteres prohibidos
    blacklist_chars = [
        'ñ', 'Ñ', '°', '-', '%',      # Otros caracteres
        'á', 'é', 'í', 'ó', 'ú',  # Vocales minúsculas con tilde
        'Á', 'É', 'Í', 'Ó', 'Ú',  # Vocales mayúsculas con tilde
        'à', 'è', 'ì', 'ò', 'ù',  # Vocales minúsculas con tilde grave
        'À', 'È', 'Ì', 'Ò', 'Ù'   # Vocales mayúsculas con tilde grave
    ]

    # Lista para almacenar los errores encontrados
    errors = []

    # Verificar si alguna columna contiene caracteres prohibidos
    for column in df.columns:
        col_name = column
        for char in blacklist_chars:
            if char in col_name:
                errors.append(f'La columna "{col_name}" contiene el carácter prohibido "{char}".')

    # Mostrar los errores encontrados, si los hay
    if errors:
        print("Errores encontrados:")
        for error in errors:
            print(error)
    else:
        print("No se encontraron errores.")
    
    
    type_mapping = {
        'int32': 'INTEGER',
        'int64': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'timestamp without time zone'
    }

    # Crear el script SQL para la tabla principal
    sql_script = f"DROP TABLE IF EXISTS {table_name};\n"
    sql_script += f"CREATE TABLE {table_name} (\n"
    sql_script += "    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,\n"
    if date_cols:
        sql_script += "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
        sql_script += "    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"

    # Generar las columnas con nombres entre comillas dobles
    for column in df.columns:
        col_name = column
        for char in blacklist_chars:
            if char in col_name:
                raise ValueError(f'La columna "{col_name}" contiene el carácter prohibido "{char}". Por favor, reemplázalo.')
        col_type = type_mapping[str(df[column].dtype)]
        if col_name == pk_name:
            sql_script += f"""    "{col_name}" {col_type} PRIMARY KEY NOT NULL,\n"""
        else:
            sql_script += f"""    "{col_name}" {col_type},\n"""

    sql_script = sql_script.rstrip(',\n') + "\n);\n"

    if date_cols and not transaction:
        sql_script += "\n-- Crear la funcion para actualizar updated_at\n"
        sql_script += f"CREATE OR REPLACE FUNCTION update_{table_name}_updated_at()\n"
        sql_script += "RETURNS TRIGGER AS $$\n"
        sql_script += "BEGIN\n"
        sql_script += "    NEW.updated_at = NOW();\n"
        sql_script += "    RETURN NEW;\n"
        sql_script += "END;\n"
        sql_script += "$$ LANGUAGE plpgsql;\n"

        sql_script += "\n-- Crear el trigger que llama a la funcion antes de actualizar\n"
        sql_script += f"CREATE TRIGGER update_{table_name}_updated_at_trigger\n"
        sql_script += f"BEFORE UPDATE ON {table_name}\n"
        sql_script += "FOR EACH ROW\n"
        sql_script += f"EXECUTE FUNCTION update_{table_name}_updated_at();\n"

    # Crear indice único si se proporciona una lista de columnas para el índice
    if index_cols:
        # verificar si las columnas del índice están presentes en el dataframe
        for col in index_cols:
            if col not in df.columns:
                raise ValueError(f"La columna indices: {col}, no está presente en el dataframe")
        # Normalizar el nombre del indice reemplazando espacios en los nombres de columnas
        index_name = '_'.join([col.replace(' ', '_').lower() for col in index_cols])
        index_cols_formatted = ', '.join([f"\"{col}\"" for col in index_cols])
        sql_script += f"\n-- Crear indice unico\n"
        sql_script += f"CREATE UNIQUE INDEX unique_{table_name}_{index_name}\n"
        sql_script += f"ON {table_name} ({index_cols_formatted});\n"

    # Crear la funcion y el trigger para la transferencia de datos
    if transaction:
        sql_script += f"\n-- Crear la funcion para transferir datos a {transaction}\n"
        sql_script += f"CREATE OR REPLACE FUNCTION transfer_data_to_{transaction}()\n"
        sql_script += "RETURNS TRIGGER AS $$\n"
        sql_script += "BEGIN\n"

        columns = ', '.join([f'"{col}"' for col in df.columns])
        values = ', '.join([f"NEW.\"{col}\"" for col in df.columns])

        if pk_name:
            # verificar si la columna pk_name está presente en el dataframe
            if pk_name not in df.columns:
                raise ValueError(f"La columna PK: {pk_name}, no está presente en el dataframe")
            # Inserción con actualización en caso de conflicto usando ON CONFLICT si pk_name está definido
            sql_script += f"    INSERT INTO {transaction} ({columns}, updated_at)\n"
            sql_script += f"    VALUES ({values}, CURRENT_TIMESTAMP)\n"
            sql_script += f"    ON CONFLICT (\"{pk_name}\") DO UPDATE SET\n"
            update_set = ', '.join([f"\"{col}\" = EXCLUDED.\"{col}\"" for col in df.columns if col != pk_name])
            sql_script += f"        {update_set},\n"
            sql_script += "        updated_at = CURRENT_TIMESTAMP;\n"
        else:
            # Simple inserción si no hay pk_name definido
            sql_script += f"    INSERT INTO {transaction} ({columns}, updated_at)\n"
            sql_script += f"    VALUES ({values}, CURRENT_TIMESTAMP);\n"

        sql_script += "    RETURN NEW;\n"
        sql_script += "END;\n"
        sql_script += "$$ LANGUAGE plpgsql;\n"

        sql_script += f"\n-- Crear el trigger que llama a la funcion despues de insertar\n"
        sql_script += f"CREATE TRIGGER transfer_data_to_{transaction}_trigger\n"
        sql_script += f"AFTER INSERT ON {table_name}\n"
        sql_script += "FOR EACH ROW\n"
        sql_script += f"EXECUTE FUNCTION transfer_data_to_{transaction}();\n"

    # Guardar el script en un archivo
    with open(file_name, 'w') as f:
        f.write(sql_script)

    print("El archivo SQL con el trigger ha sido generado exitosamente.")

def generate_transaction_script(df, table_name, error_table_name, file_name, primary_key_column=None):
    """
    Genera un script SQL dinámico para insertar filas de un DataFrame en PostgreSQL con manejo de errores.
    Si se proporciona un primary_key_column, se agrega un ON CONFLICT DO UPDATE en el query.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos a insertar.
        table_name (str): Nombre de la tabla destino.
        error_table_name (str): Nombre de la tabla para registrar errores.
        primary_key_column (str, optional): Nombre de la columna de la clave primaria para manejar conflictos.
    
    Returns:
        str: Script SQL.
    """
    # Obtener columnas del DataFrame
    columns = df.columns.tolist()
    
    # Generar la lista de columnas en formato SQL
    columns_sql = ", ".join(columns)
    
    # Generar las filas en formato SQL VALUES
    values = []
    for row in df.itertuples(index=False):
        row_values = ", ".join(
            f"'{x}'" if isinstance(x, str) else "NULL" if pd.isna(x) else str(x)
            for x in row
        )
        values.append(f"({row_values})")
    
    values_sql = ",\n            ".join(values)

    # Generar la parte del ON CONFLICT si se pasa la columna de la clave primaria
    on_conflict_sql = ""
    if primary_key_column:
        on_conflict_sql = f"""
            ON CONFLICT ({primary_key_column}) 
            DO UPDATE SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key_column)}
        """

    # Generar el script SQL
    script = f"""
DO $$
DECLARE
    fila RECORD;
BEGIN
    -- Iterar sobre las filas a insertar
    FOR fila IN
        SELECT * FROM (VALUES
            {values_sql}
        ) AS datos({', '.join(columns)})
    LOOP
        BEGIN
            -- Intentar insertar la fila
            INSERT INTO {table_name} ({columns_sql})
            VALUES ({', '.join(f'fila.{col}' for col in columns)}) {on_conflict_sql};

        EXCEPTION
            WHEN unique_violation THEN
                -- Capturar el conflicto y registrar el error con detalles específicos
                INSERT INTO {error_table_name} (table_name, error_message, data)
                VALUES (
                    '{table_name}', 
                    'Unique violation error: Conflict in columns {', '.join(columns)}', 
                    row_to_json(fila)
                );
            WHEN foreign_key_violation THEN
                -- Error de clave foránea
                INSERT INTO {error_table_name} (table_name, error_message, data)
                VALUES (
                    '{table_name}', 
                    'Foreign key violation error: Invalid foreign key value in row', 
                    row_to_json(fila)
                );
            WHEN check_violation THEN
                -- Error de violación de restricciones de CHECK
                INSERT INTO {error_table_name} (table_name, error_message, data)
                VALUES (
                    '{table_name}', 
                    'Check constraint violation error: Invalid value in row',
                    row_to_json(fila)
                );
            WHEN others THEN
                -- Registrar error general con detalles
                INSERT INTO {error_table_name} (table_name, error_message, data)
                VALUES (
                    '{table_name}', 
                    'General error: ' || SQLERRM || ' | Row data: ' || row_to_json(fila)::text, 
                    row_to_json(fila)
                );
        END;
    END LOOP;
END $$;
"""
    with open(file_name, "w") as file:
        file.write(script)
    return script

