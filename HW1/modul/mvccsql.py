import sqlite3
import pandas as pd
from datetime import datetime


class MVCCSQL:
    """
    Класс для предобработки данных автомобилей.
    """

    def __init__(self, BDpath = 'city_info.db'):
        self.conn = sqlite3.connect(BDpath)
        self.cursor = self.conn.cursor()
        
    def delete_all_table(self):
        self.cursor.execute("DELETE FROM lau_code_city")
        self.cursor.execute("DELETE FROM variable_code")
        self.cursor.execute("DELETE FROM estat_urb_cpop1")    
        # self.cursor.execute("DELETE FROM streets") 
        self.conn.commit()    
        
    def drop_all_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS estat_urb_cpop1")
        self.cursor.execute("DROP TABLE IF EXISTS lau_code_city")
        self.cursor.execute("DROP TABLE IF EXISTS variable_code")    
        # self.cursor.execute("DROP TABLE IF EXISTS streets") 
        self.conn.commit()
           
     
    def create_table(self):
        # Создаем таблицы
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS lau_code_city (
                id INTEGER PRIMARY KEY,
                city_id TEXT UNIQUE NOT NULL,
                city_name TEXT NOT NULL
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS variable_code (
                id INTEGER PRIMARY KEY,
                code_id TEXT UNIQUE NOT NULL,
                label TEXT NOT NULL
            )
        ''')
        
        # self.cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS estat_urb_cecfi (
        #         id INTEGER PRIMARY KEY,
        #         variable_code_id TEXT NOT NULL,
        #         city_id TEXT UNIQUE NOT NULL,
        #         year INTEGER NOT NULL,
        #         value REAL,
        #         UNIQUE(variable_code_id, city_id, year)
        #     )
        # ''')   

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS estat_urb_cpop1 (
                id INTEGER PRIMARY KEY,
                variable_code_id TEXT NOT NULL,
                city_id TEXT NOT NULL,
                year INTEGER NOT NULL,
                value REAL,
                UNIQUE(variable_code_id, city_id, year)
            )
        ''')       
        
        # self.cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS estat_urb_cecfi (
        #         id INTEGER PRIMARY KEY,
        #         variable_code_id TEXT UNIQUE NOT NULL,
        #         city_id TEXT UNIQUE NOT NULL,
                
        #         1991
        #         code_id TEXT UNIQUE NOT NULL,
        #         label TEXT NOT NULL
        #     )
        # ''')        

        self.conn.commit()
        
        
    def insert_code_city(self, df) -> int:
        print(df.shape)
        print("def insert_code_city(self, df) -> int:")
        """
        """
        data = df[['CITY ID', 'CITY NAME']].values.tolist()
        self.cursor.executemany(
        'INSERT OR REPLACE INTO lau_code_city (city_id, city_name) VALUES (?, ?)',
        data
        )
        self.conn.commit()
        return self.cursor.lastrowid        
        
    def insert_population_code(self, df) -> int:
        """
        """
        data = df[['Variable Code', 'Label']].values.tolist()
        self.cursor.executemany(
        'INSERT OR REPLACE INTO variable_code (code_id, label) VALUES (?, ?)',
        data
        )
        self.conn.commit()     
        
    def insert_estat_urb_cecfi(self, df) -> int:
        """
        """
        
        data = df[['variable_code_id', 'city_id', 'year', 'value']].values.tolist()
        self.cursor.executemany('''
            INSERT OR REPLACE INTO estat_urb_cecfi (variable_code_id, city_id, year, value)
            VALUES (?, ?, ?, ?)
        ''', data)

        self.conn.commit()
        
    def insert_estat_urb_cpop1(self, df) -> int:
        """
        Вставляет данные из DataFrame в таблицу estat_urb_cpop1,
        разбивая на чанки по 800 записей для эффективности.
        """
        self.cursor.execute("DELETE FROM estat_urb_cpop1")
        self.conn.commit() 
        
        # Выбираем нужные столбцы и конвертируем в список кортежей/списков
        data = df[['variable_code_id', 'city_id', 'year', 'value']].values.tolist()
        total = len(data)
        print(f"Всего записей для вставки: {total}")

        chunk_size = 500
        self.conn.execute('BEGIN;')  # Начинаем одну большую транзакцию

        try:
            for i in range(0, total, chunk_size):
                chunk = data[i:i + chunk_size]
                self.cursor.executemany('''
                    INSERT OR REPLACE INTO estat_urb_cpop1 (variable_code_id, city_id, year, value)
                    VALUES (?, ?, ?, ?)
                ''', chunk)
            self.conn.commit()
            print("Данные успешно вставлены.")
            return total
        except Exception as e:
            self.conn.rollback()
            print(f"Ошибка при вставке данных: {e}")
            raise
        
    def insert_estat_urb_cpop1_old(self, df) -> int:
        """
        """
        # self.cursor.execute("DELETE FROM estat_urb_cpop1")
        # self.conn.commit() 
        
        # data = df[['variable_code_id', 'city_id', 'year', 'value']].values.tolist()
        data = df[['variable_code_id', 'city_id', 'year', 'value']].iloc[:5000].values.tolist()
        # print(f"insert data len: {len(data)}")
        # self.cursor.executemany('''
        #     INSERT OR REPLACE INTO estat_urb_cpop1 (variable_code_id, city_id, year, value)
        #     VALUES (?, ?, ?, ?)
        # ''', data)
        self.conn.execute('BEGIN;')  # или self.conn.isolation_level = None и вручную BEGIN/COMMIT
        self.cursor.executemany('''
            INSERT OR REPLACE INTO estat_urb_cpop1 (variable_code_id, city_id, year, value)
            VALUES (?, ?, ?, ?)
        ''', data)
        # self.conn.commit()

        self.conn.commit()           
          
        
        
    # def insert_data(self, df):
    #     for index, row in df.iterrows():            

    #         # Печатаем каждые 10000 строк + последнюю
    #         if index % 10000 == 0 or index == len(df) - 1:
    #             print(f"index: {index} COLLISION_ID: {row['COLLISION_ID']}")
            
    #         crash_id = row['COLLISION_ID']
    #         if pd.isna(crash_id):
    #             continue  # пропускаем, если нет ID
            
    #         # Проверяем, существует ли уже запись
    #         self.cursor.execute("SELECT crash_id FROM crashes WHERE crash_id = ?", (crash_id,))
    #         result = self.cursor.fetchone()
    #         if result:
    #             continue  # пропускаем
            
    #         # Основная запись в crashes
    #         # try:
    #         self.cursor.execute('''
    #             INSERT OR REPLACE INTO crashes (
    #                 crash_id, crash_date, crash_time, borough, zip_code,
    #                 latitude, longitude, location,
    #                 on_street_id, cross_street_id, off_street_id,
    #                 number_of_persons_injured, number_of_persons_killed,
    #                 number_of_pedestrians_injured, number_of_pedestrians_killed,
    #                 number_of_cyclist_injured, number_of_cyclist_killed,
    #                 number_of_motorist_injured, number_of_motorist_killed,
    #                 contributing_factor_vehicle_1_id, contributing_factor_vehicle_2_id, contributing_factor_vehicle_3_id,
    #                 contributing_factor_vehicle_4_id, contributing_factor_vehicle_5_id,
    #                 vehicle_type_1_id, vehicle_type_2_id, vehicle_type_3_id, vehicle_type_4_id, vehicle_type_5_id
    #             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #         ''', (
    #             int(crash_id),
    #             row['CRASH DATE'],
    #             row['CRASH TIME'],
    #             row['BOROUGH'],
    #             int(row['ZIP CODE']) if not pd.isna(row['ZIP CODE']) else None,
    #             float(row['LATITUDE']) if not pd.isna(row['LATITUDE']) else None,
    #             float(row['LONGITUDE']) if not pd.isna(row['LONGITUDE']) else None,
    #             row['LOCATION'],
    #             self.get_or_create_street_id(row['ON STREET NAME']),
    #             self.get_or_create_street_id(row['CROSS STREET NAME']),
    #             self.get_or_create_street_id(row['OFF STREET NAME']),
    #             int(row['NUMBER OF PERSONS INJURED']),
    #             int(row['NUMBER OF PERSONS KILLED']),
    #             int(row['NUMBER OF PEDESTRIANS INJURED']),
    #             int(row['NUMBER OF PEDESTRIANS KILLED']),
    #             int(row['NUMBER OF CYCLIST INJURED']),
    #             int(row['NUMBER OF CYCLIST KILLED']),
    #             int(row['NUMBER OF MOTORIST INJURED']),
    #             int(row['NUMBER OF MOTORIST KILLED']),
    #             self.get_or_create_contributing_id(row['CONTRIBUTING FACTOR VEHICLE 1']),
    #             self.get_or_create_contributing_id(row['CONTRIBUTING FACTOR VEHICLE 2']),
    #             self.get_or_create_contributing_id(row['CONTRIBUTING FACTOR VEHICLE 3']),
    #             self.get_or_create_contributing_id(row['CONTRIBUTING FACTOR VEHICLE 4']),
    #             self.get_or_create_contributing_id(row['CONTRIBUTING FACTOR VEHICLE 5']),
    #             self.get_or_create_vehicle_id(row['VEHICLE TYPE CODE 1']),
    #             self.get_or_create_vehicle_id(row['VEHICLE TYPE CODE 2']),
    #             self.get_or_create_vehicle_id(row['VEHICLE TYPE CODE 3']),
    #             self.get_or_create_vehicle_id(row['VEHICLE TYPE CODE 4']),
    #             self.get_or_create_vehicle_id(row['VEHICLE TYPE CODE 5'])
    #         ))
    #         # except Exception as e:
    #         #     print(f"Ошибка при вставке crash_id {crash_id}: {e}")   
    #     self.conn.commit()   
    

    # def get_or_create_key(self,  table: str, colum: str, value: str) -> int:
    #     """
    #     Возвращает id факта из таблицы street.
    #     Если факт не существует — создаёт новую запись и возвращает новый id.
    #     """
    #     if pd.isna(value) or value == "":
    #         return None
        
    #     # Проверяем, существует ли уже запись
    #     query = "SELECT id FROM " + table + " WHERE " + colum + " = ?"
    #     self.cursor.execute(query, (value,))
    #     row = self.cursor.fetchone()
        
    #     if row:
    #         return row[0]  # Возвращаем существующий id
    #     else:
    #         # Создаём новую запись
    #         query = "INSERT INTO " + table + "  (" + colum + ") VALUES (?)"
    #         self.cursor.execute(query, (value,))
    #         self.conn.commit()               
    #         return self.cursor.lastrowid  # Возвращаем id новой записи  

    # def get_or_create_contributing_id(self, factor_text: str) -> int:
    #     return self.get_or_create_key("contributing_factors","factor_text", factor_text)

    # def get_or_create_vehicle_id(self, vehicle_type: str) -> int:
    #     return self.get_or_create_key("vehicle_types","vehicle_type", vehicle_type)
    
    # def get_or_create_street_id(self, street_name: str) -> int:
    #     return self.get_or_create_key("streets","street_name", street_name)
    
    def sql_to_df(self, query: str) -> pd.DataFrame:
        """
        Выполняет SQL-запрос и возвращает результат как pandas DataFrame.
        
        :param query: SQL-запрос (строка)
        :return: pandas.DataFrame с результатом запроса
        """
        try:
            df = pd.read_sql_query(query, self.conn)
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return pd.DataFrame()  # или raise, если нужно прервать       
        return df
    
    # ============================================================================================================================
    # def insert_data_fast(self, df):
    #     """
    #     Оптимизированная вставка данных с кэшированием и пакетной обработкой.
    #     """
    #     # ----------------------------------------------------------------------------------
    #     # Дополнительные проверки
    #     def safe_int(val):
    #         """Безопасно конвертирует значение в int. Возвращает None при ошибке."""
    #         if pd.isna(val):
    #             return None
    #         try:
    #             # Поддержка float: 10001.0 → 10001
    #             return int(float(val))
    #         except (ValueError, TypeError):
    #             pass
    #         if isinstance(val, str):
    #             val_clean = val.strip()
    #             if val_clean.isdigit():
    #                 return int(val_clean)
    #         return None

    #     def safe_float(val):
    #         """Безопасно конвертирует значение в float. Возвращает None при ошибке."""
    #         if pd.isna(val):
    #             return None
    #         try:
    #             return float(val)
    #         except (ValueError, TypeError):
    #             return None
            
    #     # Обработка справочников — собираем новые значения
    #     def process_value_old(value):
    #         if pd.isna(value) or value == "":
    #             return None
    #         if isinstance(value, str):
    #             value = value.strip()
    #             if value == "":
    #                 return None
    #         return value
        
    #     def process_value(value):
    #         # Быстрая проверка на None
    #         if value is None:
    #             return None

    #         # Если строка — обрабатываем или не интересно
    #         if isinstance(value, str):
    #             value = value.strip()
    #             return value if value != "" else None
    #         else:
    #             return None
        
    #     # Очистка от Nan
    #     def clean_set(values):
    #         return {x for x in values if pd.notna(x)}
    #     # -------------------------------------------------------------------        
        
    #     if df.empty:
    #         print("DataFrame пустой — ничего не вставляем")
    #         return

    #     print(f" Начинаем вставку {len(df)} записей...")
        
    #     # === Кэшируем все уже существующие crashs ID ===
    #     all_crash_ids = df['COLLISION_ID'].dropna().astype(int).tolist()
    #     existing_ids = self._all_exist_crashs(all_crash_ids)

    #     # === Кэшируем все существующие ID из справочников один раз ===
    #     self._preload_cache()

    #     # === Собираем новые записи для справочников ===
    #     new_streets = set()
    #     new_factors = set()
    #     new_vehicle_types = set()

    #     # Собираем данные для вставки в crashes
    #     crashes_data = []

    #     skipped_count = 0
        
    #     lenth_data = len(df)
        
    #     import pandas as pd

    #     for index, row in df.iterrows():
            
    #         # Печатаем каждые 10000 строк + последнюю
    #         if index % 100000 == 0 or index == lenth_data - 1:
    #             print(f"index: {index}")
            
    #         crash_id = row['COLLISION_ID']
    #         if pd.isna(crash_id):
    #             skipped_count += 1
    #             continue

    #         # Проверка дубликата crash_id
    #         if crash_id in existing_ids:
    #             skipped_count += 1
    #             continue

    #         # Собираем значения для справочников
    #         street_names = [
    #             process_value(row.get('ON STREET NAME')),
    #             process_value(row.get('CROSS STREET NAME')),
    #             process_value(row.get('OFF STREET NAME')),
    #         ]

    #         factor_texts = [
    #             process_value(row.get('CONTRIBUTING FACTOR VEHICLE 1')),
    #             process_value(row.get('CONTRIBUTING FACTOR VEHICLE 2')),
    #             process_value(row.get('CONTRIBUTING FACTOR VEHICLE 3')),
    #             process_value(row.get('CONTRIBUTING FACTOR VEHICLE 4')),
    #             process_value(row.get('CONTRIBUTING FACTOR VEHICLE 5')),
    #         ]

    #         vehicle_types = [
    #             process_value(row.get('VEHICLE TYPE CODE 1')),
    #             process_value(row.get('VEHICLE TYPE CODE 2')),
    #             process_value(row.get('VEHICLE TYPE CODE 3')),
    #             process_value(row.get('VEHICLE TYPE CODE 4')),
    #             process_value(row.get('VEHICLE TYPE CODE 5')),
    #         ]

    #         # Добавляем новые значения в сеты
    #         new_streets.update(clean_set(street_names))
    #         new_factors.update(clean_set(factor_texts))
    #         new_vehicle_types.update(clean_set(vehicle_types))
            
    #         # Подготавливаем данные для crashes (пока без ID — заполним после вставки справочников)
    #         crashes_data.append({
    #             'crash_id': safe_int(crash_id),  # уже int, но на всякий случай
    #             'crash_date': row['CRASH DATE'],
    #             'crash_time': row['CRASH TIME'],
    #             'borough': row['BOROUGH'],
    #             'zip_code': safe_int(row['ZIP CODE']),
    #             'latitude': safe_float(row['LATITUDE']),
    #             'longitude': safe_float(row['LONGITUDE']),
    #             'location': row['LOCATION'],
    #             'street_names': street_names,
    #             'factor_texts': factor_texts,
    #             'vehicle_types': vehicle_types,
    #             'injuries': [
    #                 safe_int(row['NUMBER OF PERSONS INJURED']),
    #                 safe_int(row['NUMBER OF PERSONS KILLED']),
    #                 safe_int(row['NUMBER OF PEDESTRIANS INJURED']),
    #                 safe_int(row['NUMBER OF PEDESTRIANS KILLED']),
    #                 safe_int(row['NUMBER OF CYCLIST INJURED']),
    #                 safe_int(row['NUMBER OF CYCLIST KILLED']),
    #                 safe_int(row['NUMBER OF MOTORIST INJURED']),
    #                 safe_int(row['NUMBER OF MOTORIST KILLED']),
    #             ]
    #         })

    #     print(f" Собрано {len(crashes_data)} записей для вставки. Пропущено: {skipped_count}")

    #     # === Вставляем новые значения в справочники пачками ===
    #     self._bulk_insert_streets(new_streets)
    #     self._bulk_insert_factors(new_factors)
    #     self._bulk_insert_vehicle_types(new_vehicle_types)

    #     # === Обновляем кэш после вставки новых значений ===
    #     self._preload_cache()

    #     # === Подготавливаем финальные данные для crashes с ID ===
    #     final_crashes_data = []

    #     for record in crashes_data:
    #         # Получаем ID из кэша
    #         on_street_id = self.street_cache.get(record['street_names'][0]) if len(record['street_names']) > 0 else None
    #         cross_street_id = self.street_cache.get(record['street_names'][1]) if len(record['street_names']) > 1 else None
    #         off_street_id = self.street_cache.get(record['street_names'][2]) if len(record['street_names']) > 2 else None

    #         factor_ids = [
    #             self.factor_cache.get(ft) for ft in record['factor_texts']
    #         ] + [None] * (5 - len(record['factor_texts']))  # дополняем до 5

    #         vehicle_ids = [
    #             self.vehicle_cache.get(vt) for vt in record['vehicle_types']
    #         ] + [None] * (5 - len(record['vehicle_types']))  # дополняем до 5

    #         # Формируем кортеж для вставки
    #         final_crashes_data.append((
    #             record['crash_id'],
    #             record['crash_date'],
    #             record['crash_time'],
    #             record['borough'],
    #             record['zip_code'],
    #             record['latitude'],
    #             record['longitude'],
    #             record['location'],
    #             on_street_id,
    #             cross_street_id,
    #             off_street_id,
    #             record['injuries'][0],  # persons injured
    #             record['injuries'][1],  # persons killed
    #             record['injuries'][2],  # pedestrians injured
    #             record['injuries'][3],  # pedestrians killed
    #             record['injuries'][4],  # cyclist injured
    #             record['injuries'][5],  # cyclist killed
    #             record['injuries'][6],  # motorist injured
    #             record['injuries'][7],  # motorist killed
    #             factor_ids[0], factor_ids[1], factor_ids[2], factor_ids[3], factor_ids[4],
    #             vehicle_ids[0], vehicle_ids[1], vehicle_ids[2], vehicle_ids[3], vehicle_ids[4]
    #         ))

    #     # === Пакетная вставка в crashes ===
    #     if final_crashes_data:
    #         self.cursor.executemany('''
    #             INSERT OR REPLACE INTO crashes (
    #                 crash_id, crash_date, crash_time, borough, zip_code,
    #                 latitude, longitude, location,
    #                 on_street_id, cross_street_id, off_street_id,
    #                 number_of_persons_injured, number_of_persons_killed,
    #                 number_of_pedestrians_injured, number_of_pedestrians_killed,
    #                 number_of_cyclist_injured, number_of_cyclist_killed,
    #                 number_of_motorist_injured, number_of_motorist_killed,
    #                 contributing_factor_vehicle_1_id, contributing_factor_vehicle_2_id,
    #                 contributing_factor_vehicle_3_id, contributing_factor_vehicle_4_id,
    #                 contributing_factor_vehicle_5_id,
    #                 vehicle_type_1_id, vehicle_type_2_id, vehicle_type_3_id,
    #                 vehicle_type_4_id, vehicle_type_5_id
    #             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #         ''', final_crashes_data)

    #         self.conn.commit()
    #         print(f" Успешно вставлено {len(final_crashes_data)} записей в crashes")
            
    # def _all_exist_crashs(self, all_crash_ids):
    #     def chunked(lst, n):
    #         for i in range(0, len(lst), n):
    #             yield lst[i:i + n]

    #     existing_ids = set()
    #     for chunk in chunked(all_crash_ids, 1000):
    #         placeholders = ','.join('?' * len(chunk))
    #         query = f"SELECT crash_id FROM crashes WHERE crash_id IN ({placeholders})"
    #         self.cursor.execute(query, chunk)
    #         existing_ids.update(row[0] for row in self.cursor.fetchall())
    #     return existing_ids

    # def _preload_cache(self):
    #     """Загружает все существующие ID из справочников в кэш"""
    #     self.street_cache = {}
    #     self.factor_cache = {}
    #     self.vehicle_cache = {}

    #     # Улицы
    #     self.cursor.execute("SELECT street_name, id FROM streets")
    #     for name, id in self.cursor.fetchall():
    #         self.street_cache[name] = id

    #     # Факторы
    #     self.cursor.execute("SELECT factor_text, id FROM contributing_factors")
    #     for text, id in self.cursor.fetchall():
    #         self.factor_cache[text] = id

    #     # Типы ТС
    #     self.cursor.execute("SELECT vehicle_type, id FROM vehicle_types")
    #     for vt, id in self.cursor.fetchall():
    #         self.vehicle_cache[vt] = id

    #     print(f" Кэш загружен: улицы({len(self.street_cache)}), факторы({len(self.factor_cache)}), типы ТС({len(self.vehicle_cache)})")

    # def _bulk_insert_streets(self, street_names):
    #     """Пакетная вставка новых улиц"""
    #     if not street_names:
    #         return
    #     # Фильтруем только те, которых нет в кэше
    #     new_names = [name for name in street_names if name not in self.street_cache]
    #     if not new_names:
    #         return
    #     self.cursor.executemany(
    #         "INSERT OR IGNORE INTO streets (street_name) VALUES (?)",
    #         [(name,) for name in new_names]
    #     )
    #     self.conn.commit()
    #     print(f" Добавлено {len(new_names)} новых улиц")

    # def _bulk_insert_factors(self, factor_texts):
    #     """Пакетная вставка новых факторов"""
    #     if not factor_texts:
    #         return
    #     new_texts = [text for text in factor_texts if text not in self.factor_cache]
    #     if not new_texts:
    #         return
    #     self.cursor.executemany(
    #         "INSERT OR IGNORE INTO contributing_factors (factor_text) VALUES (?)",
    #         [(text,) for text in new_texts]
    #     )
    #     self.conn.commit()
    #     print(f" Добавлено {len(new_texts)} новых факторов")

    # def _bulk_insert_vehicle_types(self, vehicle_types):
    #     """Пакетная вставка новых типов ТС"""
    #     if not vehicle_types:
    #         return
    #     new_types = [vt for vt in vehicle_types if vt not in self.vehicle_cache]
    #     if not new_types:
    #         return
    #     self.cursor.executemany(
    #         "INSERT OR IGNORE INTO vehicle_types (vehicle_type) VALUES (?)",
    #         [(vt,) for vt in new_types]
    #     )
    #     self.conn.commit()
    #     print(f" Добавлено {len(new_types)} новых типов ТС")    
    
      
