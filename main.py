import os
import shutil
import csv
import garth
from dotenv import load_dotenv
from garminconnect import Garmin
from datetime import date, timedelta

# 1. Cargar variables de entorno
load_dotenv()

EMAIL = os.getenv("GARMIN_EMAIL")
PASSWORD = os.getenv("GARMIN_PASSWORD")
TOKEN_DIR = os.path.expanduser("~/.garth")

# Ruta absoluta para asegurar que el cron lo encuentra
CSV_FILE_PATH = os.path.join(os.getcwd(), "garmin_stats_history.csv")

def login_garmin():
    """Conexión robusta (MFA + Corrección de nombre)."""
    if not EMAIL or not PASSWORD:
        print("❌ Error: Credenciales no encontradas en .env")
        return None

    try:
        if os.path.exists(TOKEN_DIR):
            garth.resume(TOKEN_DIR)
            if not garth.client.profile:
                raise Exception("Perfil vacío")
    except Exception:
        if os.path.exists(TOKEN_DIR):
            shutil.rmtree(TOKEN_DIR)
        try:
            garth.login(EMAIL, PASSWORD)
            garth.save(TOKEN_DIR)
        except Exception as e:
            print(f"❌ Error en login: {e}")
            return None

    try:
        client = Garmin(EMAIL, PASSWORD)
        client.garth = garth.client
        if garth.client.profile:
            client.display_name = garth.client.profile['displayName']
        return client
    except Exception as e:
        print(f"❌ Error configurando cliente: {e}")
        return None

def get_days_data(client, days=7):
    """Descarga los últimos días."""
    data_list = []
    end_date = date.today()
    
    print(f"\n📥 Analizando datos de los últimos {days} días...")
    
    for i in range(days):
        # Vamos de más antiguo a más nuevo para orden lógico
        current_date = end_date - timedelta(days=(days - 1 - i))
        date_str = current_date.isoformat()
        
        try:
            stats = client.get_stats(date_str)
            
            sleep_hours = 0
            try:
                sleep_data = client.get_sleep_data(date_str)
                seconds = sleep_data.get('dailySleepDTO', {}).get('sleepTimeSeconds', 0)
                sleep_hours = round(seconds / 3600, 2)
            except:
                pass 

            row = {
                "Fecha": date_str,
                "Pasos": stats.get('totalSteps', 0),
                "Objetivo": stats.get('stepGoal', 0),
                "Distancia (km)": round(stats.get('totalDistanceMeters', 0) / 1000, 2),
                "Calorías Act": stats.get('activeCalories', 0),
                "Calorías Tot": stats.get('totalCalories', 0),
                "HR Reposo": stats.get('restingHeartRate', 'N/A'),
                "Sueño (h)": sleep_hours
            }
            data_list.append(row)
            
        except Exception as e:
            print(f"   [Error fecha {date_str}]: {e}")
            
    return data_list

def get_column_averages(rows):
    """Calcula la media aritmética de todas las columnas numéricas (excepto Fecha)."""
    if not rows:
        return {}
    
    averages = {}
    fieldnames = list(rows[0].keys())
    
    for field in fieldnames:
        if field == 'Fecha':
            continue
        
        values = []
        for row in rows:
            try:
                val = float(row[field])
                values.append(val)
            except (ValueError, TypeError):
                continue
        
        if values:
            avg = sum(values) / len(values)
            averages[field] = round(avg, 2)
    
    return averages

def get_headers_with_averages(fieldnames, averages):
    """Crea cabeceras con las medias incluidas."""
    headers = []
    for field in fieldnames:
        if field == 'Fecha':
            headers.append(field)
        elif field in averages:
            headers.append(f"{field} (media: {averages[field]})")
        else:
            headers.append(field)
    return headers

def update_csv_history(new_data):
    """Borra las filas de los días recuperados y reescribe todos los datos nuevos con medias en cabecera."""
    if not new_data:
        return

    file_exists = os.path.exists(CSV_FILE_PATH)
    existing_rows = []
    new_dates = set(row['Fecha'] for row in new_data)
    
    # 1. Si el archivo existe, leemos las filas que NO están en los nuevos datos
    if file_exists:
        try:
            with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['Fecha'] not in new_dates:
                        existing_rows.append(row)
        except Exception as e:
            print(f"⚠️ Error leyendo archivo existente: {e}")

    # 2. Combinamos los datos existentes (que no se van a actualizar) con los nuevos
    all_rows = existing_rows + new_data
    
    if not all_rows:
        print(f"ℹ️ No hay datos para escribir.")
        return

    # 3. Calculamos las medias de todas las columnas
    averages = get_column_averages(all_rows)
    
    # 4. Escribimos todos los datos con cabeceras que incluyen las medias
    try:
        with open(CSV_FILE_PATH, mode='w', newline='', encoding='utf-8') as f:
            fieldnames = list(new_data[0].keys())
            headers_with_averages = get_headers_with_averages(fieldnames, averages)
            
            # Escribimos la cabecera personalizada
            f.write(','.join(headers_with_averages) + '\n')
            
            # Escribimos las filas de datos
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerows(all_rows)
            
        print(f"✅ Se han actualizado {len(new_data)} registros en: {CSV_FILE_PATH}")
        print(f"   Medias calculadas: {averages}")
        
    except Exception as e:
        print(f"❌ Error escribiendo en el CSV: {e}")

def main():
    client = login_garmin()
    if not client:
        return

    # Obtenemos los últimos 7 días por seguridad (por si el script falló algún día)
    data = get_days_data(client, days=7)
    
    # Actualizamos el histórico
    update_csv_history(data)

if __name__ == "__main__":
    main()
