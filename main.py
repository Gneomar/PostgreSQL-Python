import os
from dotenv import load_dotenv
from modules.postgres_functions import probar_conexion_postgresql

def load_env():
    load_dotenv()
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    database = os.getenv("DATABASE")
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    return host, port, database, user, password

def main():
    host, port, database, user, password = load_env()
    probar_conexion_postgresql(host, password, database, user, port)

if __name__ == '__main__':
    main()