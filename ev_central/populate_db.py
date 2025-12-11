import sqlite3

DB_NAME = 'ev_central.db'

cps = [
    ("ALC1", "Plaza de los Luceros, 5", 0.60),
    ("MAD1", "Calle Gran Vía, 30", 0.65),
    ("BCN1", "Paseo de Gracia, 22", 0.63),
    ("SEV3", "Avenida de la Constitución, 10", 0.59),
    
    ("MAD2", "Paseo de la Castellana, 100", 0.66),
    ("BCN2", "Avenida Diagonal, 450", 0.64),
    ("VAL1", "Carrer de Colón, 1", 0.58),
    ("VAL2", "Ciudad de las Artes, 15", 0.59),
    ("ZAZ1", "Plaza del Pilar, 1", 0.55),
    ("BIL1", "Gran Vía de Don Diego, 50", 0.61),
    ("COR1", "Ronda de Isasa, 10 (Mezquita)", 0.57),
    ("MLG1", "Calle Larios, 20", 0.62),
]

def populate_data():
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        print("Añadiendo CPs realistas a la base de datos...")
        
        for cp in cps:
            try:
                cursor.execute(
                    "INSERT INTO ChargingPoints (cp_id, location, price_kwh) VALUES (?, ?, ?)",
                    (cp[0], cp[1], cp[2])
                )
                print(f"  > CP '{cp[0]}' creado con éxito.")
            
            except sqlite3.IntegrityError:
                cursor.execute(
                    "UPDATE ChargingPoints SET location = ?, price_kwh = ? WHERE cp_id = ?",
                    (cp[1], cp[2], cp[0])
                )
                print(f"  > CP '{cp[0]}' ya existía, actualizado con éxito.")

        conn.commit()
        print(f"\n¡Base de datos poblada/actualizada con {len(cps)} CPs!")

    except sqlite3.Error as e:
        print(f"Error al poblar la base de datos: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    import init_db
    init_db.create_tables()
    
    populate_data()