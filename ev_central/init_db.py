import sqlite3

DB_NAME = 'ev_central.db'

def create_tables():
    """
    Se conecta a la BBDD (la crea si no existe) y genera las tablas necesarias.
    """
    conn = None
    try:
       
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ChargingPoints (
                cp_id TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                price_kwh REAL NOT NULL,
                status TEXT DEFAULT 'DESCONECTADO',
                last_heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_update DATETIME DEFAULT CURRENT_TIMESTAMP,
                encryption_key TEXT  -- <--- NUEVO CAMPO
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Drivers (
                driver_id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ChargeLog (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cp_id TEXT NOT NULL,
                driver_id TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                total_kwh REAL NOT NULL,
                total_euros REAL NOT NULL,
                FOREIGN KEY (cp_id) REFERENCES ChargingPoints (cp_id),
                FOREIGN KEY (driver_id) REFERENCES Drivers (driver_id)
            )
        """)
        
        conn.commit()
        print(f"Base de datos '{DB_NAME}' y tablas creadas con éxito.")

    except sqlite3.Error as e:
        print(f"Error al inicializar la base de datos: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    create_tables()