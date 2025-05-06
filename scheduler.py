# ==============================================================================
# MASTER ETL SCRIPT (Python Automation - User Input Schedule Time)
# Barcha bosqichlarni navbatma-navbat bajaradi va foydalanuvchi kiritgan vaqtda
# har kuni ishga tushishga harakat qiladi (agar skript ishlab tursa).
# ==============================================================================
import requests
import pandas as pd
import os
import io
import json
import time     # <<< schedule va kutish uchun
import schedule # <<< Rejalashtirish uchun
import pyodbc
import datetime
import numpy as np
import re

print("--- MASTER ETL SCRIPT with User Input Schedule --- START ---")
# ... (Global Konfiguratsiyalar: REPO_RAW_BASE, COLUMN_MAP_URL, Papkalar, DB sozlamalari - AVVALGIDEK QOLADI) ...
# GitHub
REPO_RAW_BASE = "https://raw.githubusercontent.com/odilbekmarimov/DemoProject/main/files_final"
COLUMN_MAP_URL = "https://raw.githubusercontent.com/odilbekmarimov/DemoProject/main/column_table_map.json"
# Papkalar
RAW_DIR = "raw_data"; DECODED_DIR = "decoded_data"; CLEANED_DIR = "cleaned_data"
os.makedirs(RAW_DIR, exist_ok=True); os.makedirs(DECODED_DIR, exist_ok=True); os.makedirs(CLEANED_DIR, exist_ok=True)
# SQL Server
DB_SERVER = 'WIN-A5I5TM5OKQQ\\SQLEXPRESS'; DB_DATABASE = 'BankingDB'; USE_TRUSTED_CONNECTION = True; driver = '{ODBC Driver 17 for SQL Server}'


# --- Yordamchi Funksiyalar (get_sql_type, convert_value - AVVALGIDEK QOLADI) ---
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype): return 'BIGINT'
    elif pd.api.types.is_float_dtype(dtype): return 'FLOAT'
    elif pd.api.types.is_datetime64_any_dtype(dtype): return 'DATETIME2'
    elif pd.api.types.is_bool_dtype(dtype): return 'BIT'
    else: return 'NVARCHAR(MAX)'

def convert_value(item): # NumPy/Pandas ni Python ga o'girish
    if pd.isna(item): return None
    if isinstance(item, (np.integer)): return int(item)
    if isinstance(item, (np.floating)): return float(item)
    if isinstance(item, (np.bool_)): return bool(item)
    return item

# === ETL BOSQICHLARI UCHUN FUNKSIYALAR (Bularni to'ldirgan bo'lishingiz kerak!) ===
def run_step1_ingest_decode_metadata():
    print("\n--- Running Step 1: Ingestion, Decoding & Metadata ---")
    # >>> BU YERGA SIZNING TO'LIQ ISHLAYDIGAN Skript 1 KODINGIZ KELADI <<<
    # U `dataframes_ingested` lug'atini qaytarishi va muvaffaqiyat/xato statusini
    # print qilishi kerak.
    print("    (Step 1: Placeholder - Implement your Ingestion logic here)")
    # Misol uchun:
    dataframes_placeholder = {'users': pd.DataFrame({'id':[1], 'name': ['Test User']})}
    # Fayllarni yuklash, dekodlash va metadata yozish logikasi to'liq bo'lishi kerak
    # Metadata uchun ulanish va yozishni unutmang
    print("--- Step 1 Finished ---")
    return dataframes_placeholder # Yoki None agar xato bo'lsa

def run_step2_cleaning(dataframes_to_clean):
    print("\n--- Running Step 2: Data Cleaning ---")
    if dataframes_to_clean is None: return None
    # >>> BU YERGA SIZNING TO'LIQ ISHLAYDIGAN Skript 2 KODINGIZ KELADI <<<
    print("    (Step 2: Placeholder - Implement your Cleaning logic here)")
    cleaned_dataframes_placeholder = {}
    for name, df_orig in dataframes_to_clean.items():
        cleaned_df = df_orig.copy() # Haqiqiy tozalash funksiyalarini chaqiring
        cleaned_dataframes_placeholder[name] = cleaned_df
        save_path = os.path.join(CLEANED_DIR, f"cleaned_{name}.csv")
        if not cleaned_df.empty: cleaned_df.to_csv(save_path, index=False, encoding='utf-8')
    print("--- Step 2 Finished ---")
    return cleaned_dataframes_placeholder # Yoki None agar xato bo'lsa

def run_step3_load_to_sql(data_to_load):
    print("\n--- Running Step 3: Load to SQL Server ---")
    if data_to_load is None: return False
    # >>> BU YERGA SIZNING TO'LIQ ISHLAYDIGAN Skript 3 KODINGIZ KELADI <<<
    # (Jadvallarni alohida yuklash va PK/FK qo'shish versiyasi)
    print("    (Step 3: Placeholder - Implement your SQL Load logic here)")
    print("--- Step 3 Finished ---")
    return True # Yoki False agar xato bo'lsa

def run_step5_sql_views():
    print("\n--- Running Step 5: Create SQL Views ---")
    # >>> BU YERGA SIZNING VIEW YARATISH SQL KODINGIZNI pyodbc ORQALI BAJARADIGAN LOGIKA KELADI <<<
    print("    (Step 5: Placeholder - Implement your SQL View creation logic here)")
    print("--- Step 5 Finished ---")
    return True # Yoki False

# === ASOSIY ETL FUNKSIYASI ===
def run_full_etl_process():
    print(f"\n>>> ===================================================== <<<")
    print(f">>> ETL Process KICK OFF at: {datetime.datetime.now()} <<<")
    print(f">>> ===================================================== <<<")
    etl_start_time = datetime.datetime.now()
    overall_status = "SUCCESS"

    # BOSQICH 1
    print("\nStarting Step 1...")
    ingested_data = run_step1_ingest_decode_metadata()
    if ingested_data is None: overall_status = "FAILED at Step 1"

    # BOSQICH 2
    cleaned_data = None
    if overall_status == "SUCCESS":
        print("\nStarting Step 2...")
        cleaned_data = run_step2_cleaning(ingested_data)
        if cleaned_data is None: overall_status = "FAILED at Step 2"
    else: print("Skipping Step 2 due to previous errors.")

    # BOSQICH 3
    load_sql_success = False
    if overall_status == "SUCCESS":
        print("\nStarting Step 3...")
        load_sql_success = run_step3_load_to_sql(cleaned_data)
        if not load_sql_success: overall_status = "FAILED at Step 3"
    else: print("Skipping Step 3 due to previous errors.")

    # BOSQICH 5
    views_success = False
    if overall_status == "SUCCESS":
        print("\nStarting Step 5...")
        views_success = run_step5_sql_views()
        if not views_success: overall_status = "FAILED at Step 5"
    else: print("Skipping Step 5 due to previous errors.")

    etl_end_time = datetime.datetime.now()
    print(f"\n>>> ===================================================== <<<")
    print(f">>> ETL Process FINISHED at: {datetime.datetime.now()} <<<")
    print(f">>> Total ETL Duration: {etl_end_time - etl_start_time}")
    print(f">>> Overall Status: {overall_status}")
    print(f">>> ===================================================== <<<")

# === ASOSIY ISHGA TUSHIRISH VA REJALASHTIRISH QISMI ===
if __name__ == "__main__":
    print(">>> MASTER ETL SCRIPT with User Input Schedule <<<")

    # Birinchi marta darhol ishga tushirishni so'rash (ixtiyoriy)
    run_now_choice = input("ETL jarayonini hozir bir marta ishga tushiraymi? (yes/no): ").lower()
    if run_now_choice == 'yes':
        run_full_etl_process()

    # Har kunlik rejalashtirish uchun vaqtni so'rash
    scheduled_time_str = None
    while True:
        schedule_choice = input("\nETL jarayonini har kuni avtomatik ishga tushirishni rejalashtiraymi? (yes/no): ").lower()
        if schedule_choice == 'yes':
            while True:
                scheduled_time_str = input("  Ishga tushirish vaqtini HH:MM formatida kiriting (masalan, 06:00 yoki 23:15): ")
                try:
                    time.strptime(scheduled_time_str, '%H:%M') # Formatni tekshirish
                    break # To'g'ri format, sikldan chiqish
                except ValueError:
                    print("  Noto'g'ri vaqt formati! Iltimos, HH:MM ko'rinishida qayta kiriting.")
            break # Tashqi sikldan chiqish
        elif schedule_choice == 'no':
            print("Avtomatik rejalashtirish o'tkazib yuborildi.")
            break
        else:
            print("Iltimos, 'yes' yoki 'no' deb javob bering.")

    if scheduled_time_str:
        print(f"\nETL jarayoni har kuni soat {scheduled_time_str} da ishga tushishga rejalashtirildi.")
        print("Ushbu oynani/skriptni ochiq qoldiring. To'xtatish uchun Ctrl+C.")
        schedule.every().day.at(scheduled_time_str).do(run_full_etl_process)

        try:
            while True:
                schedule.run_pending()
                time.sleep(1) # Har soniyada tekshirish
        except KeyboardInterrupt:
            print("\nRejalashtiruvchi foydalanuvchi tomonidan to'xtatildi.")
        except Exception as e:
            print(f"\nRejalashtiruvchi siklida xatolik yuz berdi: {e}")
    else:
        print("Skript rejalashtirilmasdan yakunlandi.")

    script_end_time = datetime.datetime.now()
    print(f"\nMaster script started at {start_time_total_script}, ended at {script_end_time}")
    print("--- MASTER ETL SCRIPT with User Input Schedule --- END ---")