# Импорты для работы с Apache Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Импорты для работы с базами данных
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from sqlalchemy import create_engine

# Импорты для обработки ошибок
from airflow.exceptions import AirflowException
import time
from sqlalchemy.exc import OperationalError, DatabaseError
from clickhouse_driver.errors import Error as ClickHouseError
from mysql.connector.errors import Error as MySqlError
import socket

# Импорты для работы с данными и системой
import pyarrow.parquet as pq
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import pandas as pd
import os
import gc
import logging
import json
import numpy as np

# Импорты для типизации
from typing import Set, List, Tuple, Optional, Any
from types import TracebackType

# Настройка логирования
logger = logging.getLogger(__name__)

# Константы для повторных попыток
MAX_RETRIES = 3
RETRY_DELAY = 30  # секунды
CONNECTION_TIMEOUT = 300  # секунды

# Идентификаторы подключений в Airflow
MARIADB_CONN_ID = 'smokingshop'
CLICKHOUSE_CONN_ID = 'click_house_work'

# Настройки таблиц и файлов
TABLE_NAME = 'smokingshop_sales_1'
EXPORT_PATH = '/tmp/smokingshop_data_incremental_1.parquet'

# SQL запросы (остаются без изменений)
SQL_QUERY_ALL = """
SELECT 
    sr.tabular_row_id,
    sr.sbis_date, 
    sr.`number`,
    sr.document_id, 
    sr.sbis_account_id,
    sa.sbis_account_name,
    sa.is_main,
    sa.status,
    sr.nomenclature_code,
    sr.nomenclature_title, 
    sr.nomenclature_count, 
    sr.nomenclature_price, 
    sr.nomenclature_cost_price,
    sr.nomenclature_price_total, 
    sr.nomenclature_cost_price_total, 
    sr.realization_url, 
    sr.client_id, 
    sr.employee_id,
    sr.updated_at
FROM 
    smokingshop_report_1 sr
left JOIN 
    sbis_accounts sa ON 
    sr.sbis_account_id=sa.sbis_account_id
"""

SQL_QUERY_BY_KEYS = """
SELECT 
    sr.tabular_row_id,
    sr.sbis_date, 
    sr.`number`,
    sr.document_id, 
    sr.sbis_account_id,
    sa.sbis_account_name,
    sa.is_main,
    sa.status,
    sr.nomenclature_code,
    sr.nomenclature_title, 
    sr.nomenclature_count, 
    sr.nomenclature_price, 
    sr.nomenclature_cost_price,
    sr.nomenclature_price_total, 
    sr.nomenclature_cost_price_total, 
    sr.realization_url, 
    sr.client_id, 
    sr.employee_id,
    sr.updated_at
FROM 
    smokingshop_report_1 sr
left JOIN 
    sbis_accounts sa ON 
    sr.sbis_account_id=sa.sbis_account_id
WHERE 
    (sr.sbis_account_id, sr.document_id, sr.tabular_row_id) IN ({})
"""

# Декоратор для повторных попыток с экспоненциальной задержкой
def retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    retry_delay: int = RETRY_DELAY,
    exceptions: tuple = (
        OperationalError, 
        DatabaseError, 
        ClickHouseError, 
        MySqlError,
        ConnectionError,
        TimeoutError,
        socket.timeout,
        socket.error
    )
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = retry_delay * (2 ** attempt)  # Экспоненциальная задержка
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}. "
                            f"Error: {str(e)}. Retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            f"All {max_retries} attempts failed for {func.__name__}. "
                            f"Last error: {str(e)}"
                        )
                        raise AirflowException(f"Operation failed after {max_retries} retries: {str(e)}") from e
                except Exception as e:
                    # Нет повторных попыток для непредвиденных ошибок
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    raise AirflowException(f"Unexpected error: {str(e)}") from e
            return None
        return wrapper
    return decorator

# Класс для управления подключениями с обработкой ошибок
class ConnectionManager:
    def __init__(self):
        self._mysql_hook = None
        self._ch_hook = None
        self._mysql_engine = None
        self._connection_attempts = 0
        self._max_connection_attempts = 3
    
    # Получение хука MySQL с повторными попытками
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_mysql_hook(self) -> MySqlHook:
        if self._mysql_hook is None:
            logger.info("Creating MySQL hook connection")
            self._mysql_hook = MySqlHook(mysql_conn_id=MARIADB_CONN_ID)
            logger.info("MySQL hook created successfully")
        return self._mysql_hook
    
    # Получение хука ClickHouse с повторными попытками
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_clickhouse_hook(self) -> ClickHouseHook:
        if self._ch_hook is None:
            logger.info("Creating ClickHouse hook connection")
            self._ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
            # ПРОСТОЕ ТЕСТИРОВАНИЕ БЕЗ ПАРАМЕТРОВ
            client = self._ch_hook.get_conn()
            client.execute("SELECT 1")
            logger.info("ClickHouse connection tested successfully")
        return self._ch_hook
    
    # Получение SQLAlchemy engine с настройками для устойчивости
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_mysql_engine(self):
        if self._mysql_engine is None:
            mysql_hook = self.get_mysql_hook()
            conn = mysql_hook.get_connection(mysql_hook.mysql_conn_id)
            connection_string = f"mysql+mysqldb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            
            self._mysql_engine = create_engine(
                connection_string,
                pool_recycle=300,      # Уменьшаем до 5 минут для частого переподключения
                pool_pre_ping=True,   # ОБЯЗАТЕЛЬНО - проверять перед использованием проверять "живое" ли соединение
                pool_size=5,          # Базовая размерность пула 
                max_overflow=10,       # Максимум дополнительных соединений
                pool_timeout=30,       # Таймаут ожидания свободного соединения
                pool_reset_on_return='rollback',  # Добавляем сброс при возврате в пул
                connect_args={
                    'connect_timeout': CONNECTION_TIMEOUT,
                    'read_timeout': 300,           # Таймаут на чтение
                    'write_timeout': 300,          # Таймаут на запись
                },
                # Уменьшаем логирование для производительности
                echo_pool=False,
            )
            logger.info("SQLAlchemy engine created with improved settings")
        return self._mysql_engine
    
    # Принудительное обновление engine
    def refresh_mysql_engine(self):
        if self._mysql_engine:
            self._mysql_engine.dispose()
            self._mysql_engine = None
            logger.info("MySQL engine refreshed")
        return self.get_mysql_engine()
    
    # Закрытие всех соединений
    def close_connections(self):
        try:
            if self._mysql_engine:
                self._mysql_engine.dispose()
                logger.info("MySQL engine connections closed")
        except Exception as e:
            logger.warning(f"Error closing MySQL engine: {e}")
            
# Контекстный менеджер для безопасной работы с соединениями
class DatabaseConnectionContext:
    def __init__(self):
        self.connection_manager = ConnectionManager()
    
    def __enter__(self):
        return self.connection_manager
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
        self.connection_manager.close_connections()
        if exc_type is not None:
            logger.error(f"Database operation failed: {exc_val}")
        return False  # Пропускаем исключение дальше
            
#  Оптимизированное преобразование типов с экономией памяти с использованием astype(dict)
def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting data type conversion")

    # Создаем копию для безопасной модификации
    df_converted = df.copy()
    
    # Обрабатываем дату
    df_converted['sbis_date'] = pd.to_datetime(df_converted['sbis_date'])

    # Определяем типы для каждой колонки
    type_conversions = {
        # BigInt колонки
        'tabular_row_id': 'UInt64',
        'document_id': 'UInt64', 
        'client_id': 'UInt64',
        'employee_id': 'UInt64',
        
        # TinyInt колонки
        'is_main': 'uint8',
        'status': 'uint8',
    }
    
    # Применяем преобразования типов
    for col, dtype in type_conversions.items():
        if col in df_converted.columns:
            # Используем downcast для экономии памяти
            df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce', downcast='integer').fillna(0).astype(dtype)
    
    # Строковые колонки
    string_cols = [
        'number', 'sbis_account_id', 'sbis_account_name', 
        'nomenclature_code', 'nomenclature_title', 'realization_url'
    ]
    for col in string_cols:
        if col in df_converted.columns:
            df_converted[col] = df_converted[col].fillna('').astype(str)

    # Decimal колонки
    decimal_cols = [
        'nomenclature_count', 'nomenclature_price', 'nomenclature_cost_price',
        'nomenclature_price_total', 'nomenclature_cost_price_total'
    ]
    for col in decimal_cols:
        if col in df_converted.columns:
            df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce', downcast='float').fillna(0.0).astype('float64')
    
    logger.info("Optimized data type conversion completed")
    return df_converted

# Подготовка данных для вставки с оптимизацией памяти
def prepare_data_for_insert(df: pd.DataFrame) -> List[Tuple]:
    data = [
        (
            int(row['tabular_row_id']),
            row['sbis_date'],
            str(row['number']),
            int(row['document_id']),
            str(row['sbis_account_id']),
            str(row['sbis_account_name']),
            int(row['is_main']),
            int(row['status']),
            str(row['nomenclature_code']),
            str(row['nomenclature_title']),
            float(row['nomenclature_count']),
            float(row['nomenclature_price']),
            float(row['nomenclature_cost_price']),
            float(row['nomenclature_price_total']),
            float(row['nomenclature_cost_price_total']),
            str(row['realization_url']),
            int(row['client_id']),
            int(row['employee_id'])
        )
        for _, row in df.iterrows()
    ]
    return data

# Проверяем объем данных перед полной синхронизацией
def check_data_volume(**kwargs):
    try:
        with DatabaseConnectionContext() as db:
            engine = db.get_mysql_engine()
            
            count_query = "SELECT COUNT(*) as total_count FROM smokingshop_report_1"
            result = pd.read_sql(count_query, con=engine)
            total_count = result.iloc[0]['total_count']
            
            logger.info(f"Total rows in source table: {total_count}")
            
            if total_count > 10000000:  # 10 млн строк
                logger.warning(f"Large dataset detected: {total_count} rows. Consider optimizing.")
            
            ti = kwargs['ti']
            volume_info = {
                'total_count': total_count,
                'warning_level': 'CRITICAL' if total_count > 10000000 else 'WARNING' if total_count > 5000000 else 'NORMAL',
                'checked_at': datetime.now(timezone.utc).isoformat()
            }
            ti.xcom_push(key='data_volume_info', value=volume_info)
            
            return volume_info
            
    except Exception as e:
        logger.error(f"Error checking data volume: {str(e)}")
        return None
    
# Обрабатываем чанки итеративно для экономии памяти
def process_chunks_iteratively(all_chunks, export_path):
    logger.info("Processing chunks iteratively to save memory")
    
    if not all_chunks:
        return pd.DataFrame()
        
    # Обрабатываем первый чанк
    all_chunks[0].to_parquet(export_path, index=False)
    
    # Для остальных чанков используем append mode (если поддерживается)
    for i, chunk in enumerate(all_chunks[1:], 2):
        try:
            # Более эффективный подход - записываем каждый чанк отдельно
            chunk_path = f"{export_path}.chunk_{i}"
            chunk.to_parquet(chunk_path, index=False)
            
            if i % 5 == 0:
                logger.info(f"Progress: processed {i} chunks")
                
        except Exception as e:
            logger.error(f"Error processing chunk {i}: {e}")
            raise
    
    # В реальном коде нужно объединить все чанки в финальный файл
    return pd.read_parquet(export_path)

# Получение данных из MariaDB
@retry_with_backoff()
def extract_from_mariadb(**kwargs):
    try:
        max_retries=MAX_RETRIES
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        export_path = f"/tmp/smokingshop_data_incremental_1_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
        
        logger.info("Starting data extraction from MariaDB")

        with DatabaseConnectionContext() as db:
            engine = db.get_mysql_engine()

            volume_info = ti.xcom_pull(task_ids='check_data_volume', key='data_volume_info')
        
            if volume_info:
                warning_level = volume_info.get('warning_level', 'NORMAL')
                
                # Адаптивные настройки
                if warning_level in ["CRITICAL", "WARNING"]:
                    chunk_size = 50000
                    logger.info(f"Using reduced chunk size {chunk_size} for large dataset")
                else:
                    chunk_size = 100000
            else:
                chunk_size = 100000
            
            logger.info(f"Using chunk size {chunk_size}") 
            all_chunks = []
            all_keys = []
            
            chunk_number = 0
            total_rows = 0
            
            # Улучшенная обработка чанков с проверкой соединения
            for chunk_df in pd.read_sql(SQL_QUERY_ALL, con=engine, chunksize=chunk_size):
                chunk_number += 1
                
                try:
                    # Проверяем соединение перед обработкой чанка
                    with engine.connect() as test_conn:
                        test_conn.execute("SELECT 1")
                    
                    logger.info(f"Processing chunk {chunk_number} with {len(chunk_df)} rows")
                    
                    if chunk_df.empty:
                        logger.info(f"Chunk {chunk_number} is empty, skipping")
                        continue
                        
                    # Создаем ключи для дедупликации
                    chunk_keys = chunk_df[['sbis_account_id', 'document_id', 'tabular_row_id']].copy()
                    chunk_keys['key'] = chunk_keys['sbis_account_id'].astype(str) + '_' + \
                                      chunk_keys['document_id'].astype(str) + '_' + \
                                      chunk_keys['tabular_row_id'].astype(str)
                    
                    all_keys.extend(chunk_keys['key'].tolist())
                    all_chunks.append(chunk_df)
                    total_rows += len(chunk_df)
                    
                    # Периодически сохраняем прогресс для больших датасетов
                    if chunk_number % 10 == 0:
                        logger.info(f"Progress: processed {chunk_number} chunks, {total_rows} total rows")
                    
                    # Освобождаем память
                    del chunk_keys
                    del chunk_df
                    gc.collect()
                    
                except (OperationalError, DatabaseError) as e:
                    if chunk_number >= max_retries:  # Добавить ограничение
                        raise
                    logger.warning(f"Database error in chunk {chunk_number}: {str(e)}")
                    logger.info("Attempting to refresh database connection...")
                    
                    # Закрываем текущее соединение и создаем новое
                    db.close_connections()
                    engine = db.get_mysql_engine()
                    
                    logger.info("Database connection refreshed, continuing...")
                    continue
                    
                except Exception as e:
                    logger.error(f"Unexpected error in chunk {chunk_number}: {str(e)}")
                    raise
            
            if not all_chunks:
                logger.warning("No data found in MariaDB table")
                ti.xcom_push(key='mariadb_data', value=None)
                ti.xcom_push(key='mariadb_keys_path', value=None)
                ti.xcom_push(key='has_new_data', value=False)
                return
            
            logger.info(f"Combining {len(all_chunks)} chunks")
            
            # Улучшенное объединение чанков с обработкой памяти
            try:
                df_mariadb = pd.concat(all_chunks, ignore_index=True)
                logger.info(f"Successfully combined chunks. Total rows: {len(df_mariadb)}")
            except MemoryError:
                logger.warning("Memory error during concat, using iterative processing")
                # Альтернативный подход для больших датасетов
                df_mariadb = process_chunks_iteratively(all_chunks, export_path)
                all_chunks.clear()  # Освобождаем память
            
            logger.info(f"Extracted {len(df_mariadb)} rows from MariaDB in {chunk_number} chunks")
            
            # Сохраняем в файл с обработкой ошибок
            try:
                df_mariadb.to_parquet(export_path, index=False, compression='snappy')
                logger.info(f"Data saved to {export_path}")
            except Exception as e:
                logger.error(f"Error saving to parquet: {str(e)}")
                # Пробуем альтернативный формат
                csv_path = export_path.replace('.parquet', '.csv')
                df_mariadb.to_csv(csv_path, index=False)
                export_path = csv_path 
                logger.info(f"Data saved to CSV instead: {export_path}")
            
            # Формируем DataFrame с одним столбцом 'key'
            df_keys = pd.DataFrame({'key': all_keys})

            # Путь для сохранения ключей 
            keys_path_parquet = export_path.replace('.parquet', '_keys.parquet').replace('.csv', '_keys.parquet')
            keys_path_csv = export_path.replace('.parquet', '_keys.csv').replace('.csv', '_keys.csv')

            try:
                # Сохраняем в Parquet (более эффективно по размеру)
                df_keys.to_parquet(keys_path_parquet, index=False, compression='snappy')
                logger.info(f"Keys saved to Parquet: {keys_path_parquet}")
                keys_path_to_use = keys_path_parquet
            except Exception as e:
                # Если не удалось Parquet — сохраняем в CSV
                df_keys.to_csv(keys_path_csv, index=False)
                logger.warning(f"Error saving keys to Parquet, saved to CSV instead: {keys_path_csv}")
                keys_path_to_use = keys_path_csv

            # Отправляем в xcom информацию о местоположении файлов и прочую необходимую информацию: наличи новых данных и размер выгруженных данных из Maria DB
            ti.xcom_push(key='mariadb_data', value=export_path)
            ti.xcom_push(key='mariadb_keys_path', value=keys_path_to_use)
            ti.xcom_push(key='has_new_data', value=True)
            ti.xcom_push(key='total_rows_extracted', value=len(df_mariadb))
            
            logger.info(f"MariaDB data prepared successfully. Total keys: {len(all_keys)}")
            
            # Освобождаем память
            del df_mariadb
            del all_chunks
            del all_keys
            gc.collect()
            
    except Exception as e:
        logger.error(f"Error in extract_from_mariadb: {str(e)}", exc_info=True)
        raise AirflowException(f"Extraction from MariaDB failed: {str(e)}")

# Находим записи, у которых изменилась сумма nomenclature_cost_price и nomenclature_cost_price_total
@retry_with_backoff()
def find_changed_cost_records(**kwargs):
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        mariadb_data_path = ti.xcom_pull(key='mariadb_data', task_ids='extract_from_mariadb')
        
        if not mariadb_data_path or not os.path.isfile(mariadb_data_path):
            logger.info("No MariaDB data file found for cost comparison")
            ti.xcom_push(key='changed_cost_records_path', value=None)
            ti.xcom_push(key='has_changed_cost_records', value=False)
            ti.xcom_push(key='changed_cost_records_count', value=0)
            return
        
        logger.info("Starting cost price change detection using sum comparison")
        
        # Загружаем данные из MariaDB
        if mariadb_data_path.endswith('.csv'):
            df_mariadb = pd.read_csv(mariadb_data_path)
        else:
            df_mariadb = pd.read_parquet(mariadb_data_path)
        
        logger.info(f"The data from the MariaDB source for comparison is downloaded from file {mariadb_data_path}")
        
        # Создаем ключ и вычисляем сумму полей себестоимости
        df_mariadb['key'] = df_mariadb['sbis_account_id'].fillna('').astype(str) + '_' + \
                           df_mariadb['document_id'].fillna('').astype(str) + '_' + \
                           df_mariadb['tabular_row_id'].fillna('').astype(str)
        df_mariadb['cost_sum_mariadb'] = df_mariadb['nomenclature_cost_price'].fillna(0) + df_mariadb['nomenclature_cost_price_total'].fillna(0)
        
        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            
            # Получаем существующие данные из ClickHouse
            existing_data_sql = f"""
                SELECT 
                    sbis_account_id, 
                    document_id, 
                    tabular_row_id,
                    nomenclature_cost_price,
                    nomenclature_cost_price_total
                FROM {TABLE_NAME}
            """
            # Читаем данные из clickhouse по частям            
            ch_data =  []
            total_rows = 0
            
            try:
                for rows in client.execute_iter(existing_data_sql, settings={'max_block_size': 50000}):
                    ch_data.extend(rows)
                    total_rows += len(rows)
                    
                    # Логируем каждые 100к строк
                    if total_rows % 100000 == 0:
                        logger.info(f"Progress: loaded {total_rows} rows...")
                        
            except Exception as e:
                logger.error(f"Error during iterative reading: {e}")
                # Fallback: попробуем с меньшим размером блока
                ch_data = []
                try:
                    for rows in client.execute_iter(existing_data_sql, settings={'max_block_size': 10000}):
                        ch_data.extend(rows)
                        total_rows += len(rows)
                        if total_rows % 50000 == 0:
                            logger.info(f"Fallback progress: loaded {total_rows} rows...")
                except Exception as fallback_error:
                    logger.error(f"Fallback reading also failed: {fallback_error}")
                    raise
            
            logger.info(f"Successfully loaded {len(ch_data)} total rows from Clickhouse table {TABLE_NAME}")

            if not ch_data:
                logger.info("No existing data in ClickHouse for cost comparison")
                ti.xcom_push(key='changed_cost_records_path', value=None)
                ti.xcom_push(key='has_changed_cost_records', value=False)
                ti.xcom_push(key='changed_cost_records_count', value=0)
                return
            
            # Создаем DataFrame из данных ClickHouse
            # Проверяем структуру данных для отладки
            if ch_data:
                # Проверяем первый элемент
                first_item = ch_data[0]
                logger.info(f"Type of first item: {type(first_item)}")
                logger.info(f"Length of first item: {len(first_item) if hasattr(first_item, '__len__') else 'No length'}")
                
                # Если данные в неправильном формате, преобразуем их
                if not isinstance(first_item, (tuple, list)) or len(first_item) != 5:
                    logger.warning("Data format issue detected, attempting to fix...")
                    # Предполагаем, что данные приходят как плоский список и группируем по 5 элементов
                    if len(ch_data) % 10 == 0:
                        ch_data = [tuple(ch_data[i:i+5]) for i in range(0, len(ch_data), 5)]
                        logger.info(f"Data reformatted, new length: {len(ch_data)}")
                    else:
                        raise ValueError(f"Unexpected data format: cannot group into 5-column rows from {len(ch_data)} elements")
                    
            ch_df = pd.DataFrame(ch_data, columns=[
                'sbis_account_id', 'document_id', 'tabular_row_id',
                'nomenclature_cost_price', 'nomenclature_cost_price_total'
            ])
            
            # Создаем ключ и вычисляем сумму полей себестоимости
            ch_df['key'] = ch_df['sbis_account_id'].fillna('').astype(str) + '_' + \
                          ch_df['document_id'].fillna('').astype(str) + '_' + \
                          ch_df['tabular_row_id'].fillna('').astype(str)
            ch_df['cost_sum_clickhouse'] = ch_df['nomenclature_cost_price'].fillna(0) + ch_df['nomenclature_cost_price_total'].fillna(0)
            
            # Объединяем данные для сравнения
            comparison_df = df_mariadb.merge(
                ch_df[['key', 'cost_sum_clickhouse']],
                on='key',
                how='inner'  # Только существующие записи в обеих системах
            )
            
            # Приводим к числовым типам и заполняем NaN
            comparison_df['cost_sum_mariadb'] = pd.to_numeric(comparison_df['cost_sum_mariadb'], errors='coerce').fillna(0)
            comparison_df['cost_sum_clickhouse'] = pd.to_numeric(comparison_df['cost_sum_clickhouse'], errors='coerce').fillna(0)
            
            # Находим записи с измененной суммой(с допуском для float и с округлением для исключения незначительных изменений)
            tolerance=0.01
            changed_records = comparison_df[
                ~np.isclose(
                    comparison_df['cost_sum_mariadb'], 
                    comparison_df['cost_sum_clickhouse'],
                    rtol=1e-9,
                    atol=tolerance
                )
            ]
            
            if changed_records.empty:
                logger.info("No records with changed cost prices found")
                ti.xcom_push(key='changed_cost_records_path', value=None)
                ti.xcom_push(key='has_changed_cost_records', value=False)
                ti.xcom_push(key='changed_cost_records_count', value=0)
            else:
                changed_keys_path = f'/tmp/changed_cost_records_{execution_date.strftime("%Y%m%d_%H%M%S")}.parquet'
                
                # Сохраняем ключи измененных записей
                changed_keys = changed_records[['sbis_account_id', 'document_id', 'tabular_row_id']].copy()
                changed_keys.to_parquet(changed_keys_path, index=False)
                
                logger.info(f"Found {len(changed_records)} records with changed cost prices")
                ti.xcom_push(key='changed_cost_records_path', value=changed_keys_path)
                ti.xcom_push(key='has_changed_cost_records', value=True)
                ti.xcom_push(key='changed_cost_records_count', value=len(changed_records))
                
                # Логируем пример изменений для отладки
                sample_changes = changed_records[[
                    'key', 'cost_sum_mariadb', 'cost_sum_clickhouse'
                ]].head(3)
                logger.info(f"Sample cost sum changes:\n{sample_changes}")
        
    except Exception as e:
        logger.error(f"Error in find_changed_cost_records: {str(e)}", exc_info=True)
        raise AirflowException(f"Cost records change detection failed: {str(e)}")

# Удаляем из ClickHouse записи, у которых изменились цены себестоимости
@retry_with_backoff()
def delete_changed_records_from_clickhouse(**kwargs):
    try:
        ti = kwargs['ti']
        changed_cost_records_path = ti.xcom_pull(key='changed_cost_records_path', task_ids='find_changed_cost_records')
        has_changed_cost_records = ti.xcom_pull(key='has_changed_cost_records', task_ids='find_changed_cost_records')
        
        if not has_changed_cost_records or not changed_cost_records_path:
            logger.info("No changed cost records to delete")
            return
        
        logger.info("Deleting changed cost records from ClickHouse")
        
        # Загружаем ключи измененных записей
        changed_keys_df = pd.read_parquet(changed_cost_records_path)
        
        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            
            # Формируем условия для удаления
            delete_conditions = []
            for _, row in changed_keys_df.iterrows():
                condition = f"(sbis_account_id = '{row['sbis_account_id']}' AND document_id = {row['document_id']} AND tabular_row_id = {row['tabular_row_id']})"
                delete_conditions.append(condition)
            
            # Разбиваем на батчи для избежания слишком больших запросов
            batch_size = 1000
            total_deleted = 0
            
            for i in range(0, len(delete_conditions), batch_size):
                batch_conditions = delete_conditions[i:i + batch_size]
                where_clause = " OR ".join(batch_conditions)
                
                delete_sql = f"ALTER TABLE {TABLE_NAME} DELETE WHERE {where_clause}"
                
                logger.info(f"Deleting batch {i//batch_size + 1} with {len(batch_conditions)} conditions")
                client.execute(delete_sql)
                total_deleted += len(batch_conditions)
                
                # Небольшая пауза между батчами
                time.sleep(1)
            
            logger.info(f"Successfully deleted {total_deleted} changed records from ClickHouse")
            
    except Exception as e:
        logger.error(f"Error in delete_changed_records_from_clickhouse: {str(e)}", exc_info=True)
        raise AirflowException(f"Deletion of changed records failed: {str(e)}")

# Сравнение по ключам данных из Maria DB и Clickhouse: появились ли новые записи для вставки
@retry_with_backoff()
def compare_data(**kwargs):
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        keys_path = ti.xcom_pull(task_ids='extract_from_mariadb', key='mariadb_keys_path')
        
        if not keys_path:
            logger.info("No keys file found")
            ti.xcom_push(key='new_records_keys_path', value=None)
            ti.xcom_push(key='has_new_records', value=False)
            return
            
        # Загружаем ключи
        if keys_path.endswith('.parquet'):
            df_keys = pd.read_parquet(keys_path)
        else:
            df_keys = pd.read_csv(keys_path)
        
        logger.info(f"Loaded MariaDB keys from file {keys_path}")
            
        if df_keys.empty:
            logger.info("No keys to compare")
            ti.xcom_push(key='new_records_keys_path', value=None)
            ti.xcom_push(key='has_new_records', value=False)
            return
            
        logger.info(f"Starting optimized data comparison for {len(df_keys)} MariaDB keys")

        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            
            # Проверяем существование таблицы
            check_table_sql = f"""
            SELECT COUNT() FROM system.tables 
            WHERE database = currentDatabase() AND name = '{TABLE_NAME}'
            """
            table_exists = client.execute(check_table_sql)[0][0] > 0
            
            logger.info(f"Table {TABLE_NAME} has {client.execute(check_table_sql)[0][0]} records")
            
            if not table_exists:
                logger.info(f"Table {TABLE_NAME} does not exist in ClickHouse. All records are new.")
                new_keys_path = f'/tmp/new_keys_{execution_date.strftime("%Y%m%d_%H%M%S")}.parquet'
                pd.DataFrame({'key': df_keys['key'].tolist()}).to_parquet(new_keys_path, index=False)
                ti.xcom_push(key='new_records_keys_path', value=new_keys_path)
                ti.xcom_push(key='has_new_records', value=True)
                ti.xcom_push(key='new_records_count', value=len(df_keys))
                return
            
            # УПРОЩЕННЫЙ ПОДХОД: получаем все ключи из ClickHouse и сравниваем в памяти
            logger.info("Getting existing keys from ClickHouse")
            
            # Получаем все ключи из ClickHouse в том же формате
            existing_keys_sql = f"""
            SELECT 
                concat(toString(sbis_account_id), '_', toString(document_id), '_', toString(tabular_row_id)) as key
            FROM {TABLE_NAME}
            """
            
            existing_data = client.execute(existing_keys_sql)
            existing_keys_set = {row[0] for row in existing_data}
            
            logger.info(f"Found {len(existing_keys_set)} existing keys in ClickHouse")
            
            # Создаем множество ключей из MariaDB
            mariadb_keys_set = set(df_keys['key'].tolist())
            
            # Находим разницу - ключи, которые есть в MariaDB, но нет в ClickHouse
            new_keys_set = mariadb_keys_set - existing_keys_set
            
            # Сохраняем результат
            if new_keys_set:
                new_keys_path = f'/tmp/new_keys_{execution_date.strftime("%Y%m%d_%H%M%S")}.parquet'
                pd.DataFrame({'key': list(new_keys_set)}).to_parquet(new_keys_path, index=False)
                ti.xcom_push(key='new_records_keys_path', value=new_keys_path)
                ti.xcom_push(key='has_new_records', value=True)
                ti.xcom_push(key='new_records_count', value=len(new_keys_set))
                logger.info(f"Found {len(new_keys_set)} new records")
            else:
                ti.xcom_push(key='new_records_keys_path', value=None)
                ti.xcom_push(key='has_new_records', value=False)
                ti.xcom_push(key='new_records_count', value=0)
                logger.info("No new records found")
                
    except Exception as e:
        logger.error(f"Error in compare_data: {str(e)}", exc_info=True)
        raise AirflowException(f"Data comparison failed: {str(e)}")
    
# Загрузка новых данных в Clickhouse
@retry_with_backoff()
def load_incremental_to_clickhouse(**kwargs):
    try:
        ti = kwargs['ti']
        mariadb_data_path = ti.xcom_pull(key='mariadb_data', task_ids='extract_from_mariadb')
        new_records_keys_path = ti.xcom_pull(key='new_records_keys_path', task_ids='compare_data')
        has_new_records = ti.xcom_pull(key='has_new_records', task_ids='compare_data')
        
        if not has_new_records:
            logger.info("No new records to load")
            return
    
        if not mariadb_data_path or not os.path.isfile(mariadb_data_path):
            logger.warning("MariaDB data file not found or path is None")
            return
        
        if mariadb_data_path.endswith('.csv'):
            df_mariadb = pd.read_csv(mariadb_data_path)
        else:
            df_mariadb = pd.read_parquet(mariadb_data_path)
                        
        if not new_records_keys_path or not os.path.isfile(new_records_keys_path):
            logger.warning(f"New records keys file not found: {new_records_keys_path}")
            return
        
        if new_records_keys_path.endswith('.parquet'):
            df_new_keys = pd.read_parquet(new_records_keys_path)
        else:
            df_new_keys = pd.read_csv(new_records_keys_path)

        new_keys_list = df_new_keys['key'].tolist()

        if not has_new_records or not new_keys_list:
            logger.info("No new records to load. Skipping load operation.")
            return
        
        logger.info(f"Loading {len(new_keys_list)} new records to ClickHouse")
        
        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            logger.info("Connected to ClickHouse successfully")

            check_table_sql = f"""
            SELECT COUNT() as count FROM system.tables 
            WHERE database = currentDatabase() AND name = '{TABLE_NAME}'
            """
            table_exists = client.execute(check_table_sql)[0][0] > 0
            if not table_exists:
                logger.info(f"Table {TABLE_NAME} does not exist, creating it")
                create_table_sql = f"""
                CREATE TABLE {TABLE_NAME} (
                    tabular_row_id UInt64,
                    sbis_date DateTime,
                    number String,
                    document_id UInt64,
                    sbis_account_id String,
                    sbis_account_name String,
                    is_main UInt8,
                    status UInt8,
                    nomenclature_code String,
                    nomenclature_title String,
                    nomenclature_count Float64,
                    nomenclature_price Float64,
                    nomenclature_cost_price Float64,
                    nomenclature_price_total Float64,
                    nomenclature_cost_price_total Float64,
                    realization_url String,
                    client_id UInt64,
                    employee_id UInt64,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(sbis_date)
                ORDER BY (sbis_date, number, tabular_row_id)
                """
                client.execute(create_table_sql)
                logger.info("Table created successfully")

            insert_sql = f"""
                INSERT INTO {TABLE_NAME} (
                    tabular_row_id, sbis_date, number, document_id, sbis_account_id, 
                    sbis_account_name, is_main, status, nomenclature_code, nomenclature_title, 
                    nomenclature_count, nomenclature_price, nomenclature_cost_price,
                    nomenclature_price_total, nomenclature_cost_price_total,
                    realization_url, client_id, employee_id
                ) VALUES
            """

            parquet_file = pq.ParquetFile(mariadb_data_path)
            chunk_size = 50000

            total_inserted = 0
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                df_batch = batch.to_pandas()
                df_batch['key'] = df_batch['sbis_account_id'].astype(str) + '_' + \
                                  df_batch['document_id'].astype(str) + '_' + \
                                  df_batch['tabular_row_id'].astype(str)

                df_new = df_batch[df_batch['key'].isin(new_keys_list)]

                if df_new.empty:
                    continue
                
                df_new = convert_data_types(df_new)
                data = prepare_data_for_insert(df_new)
                
                client.execute(insert_sql, data)
                total_inserted += len(data)
            
            logger.info(f"Loaded a total of {total_inserted} new records into ClickHouse")
    
    except Exception as e:
        logger.error(f"Error in load_incremental_to_clickhouse: {str(e)}", exc_info=True)
        raise AirflowException(f"Load to ClickHouse failed: {str(e)}")

# Очистка временного файла
def cleanup_file(**kwargs):
    try:
        ti = kwargs['ti']
        mariadb_data_path = ti.xcom_pull(key='mariadb_data', task_ids='extract_from_mariadb')
        keys_path = ti.xcom_pull(key='mariadb_keys_path', task_ids='extract_from_mariadb')
        new_records_count = ti.xcom_pull(key='new_records_count', task_ids='compare_data')
        changed_cost_records_path = ti.xcom_pull(key='changed_cost_records_path', task_ids='find_changed_cost_records')
        new_records_keys_path = ti.xcom_pull(key='new_records_keys_path', task_ids='compare_data')
        
        if mariadb_data_path and os.path.isfile(mariadb_data_path):
            os.remove(mariadb_data_path)
            logger.info(f"Temporary file {mariadb_data_path} removed (loaded {new_records_count or 0} new records)")
        else:
            logger.info("No temporary file to remove")
            
        if keys_path and os.path.isfile(keys_path):
            os.remove(keys_path)
            logger.info(f"Temporary file {keys_path} removed")
        else:
            logger.info("No temporary file to remove")
        
        if changed_cost_records_path and os.path.isfile(changed_cost_records_path):
            os.remove(changed_cost_records_path)
            logger.info(f"Temporary file {changed_cost_records_path} removed")
        else:
            logger.info("No temporary file to remove")
            
        if new_records_keys_path and os.path.isfile(new_records_keys_path):
            os.remove(new_records_keys_path)
            logger.info(f"Temporary file {new_records_keys_path} removed")
        else:
            logger.info("No temporary file to remove")

    except Exception as e:
        logger.error(f"Error in cleanup_file: {str(e)}", exc_info=True)

# Настройки DAG с улучшенной обработкой ошибок
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'retry_exponential_backoff': True,  # Добавляем экспоненциальную задержку
    'max_retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': lambda context: logger.error(f"DAG failed: {context['exception']}"),
}

with DAG(
    dag_id='smokingshop_mariadb_to_clickhouse_improved_1',
    start_date=datetime(2024, 10, 1, 5, 15),
    schedule='15 5,7,9,11,13 * * *',
    catchup=False,
    tags=['smokingshop','mariadb', 'clickhouse', 'etl', 'incremental', 'improved'],
    max_active_runs=1,
    concurrency=1,
    default_args=default_args
) as dag:
    
    check_volume = PythonOperator(
    task_id='check_data_volume',
    python_callable=check_data_volume,
    provide_context=True
    )

    extract = PythonOperator(
        task_id='extract_from_mariadb',
        python_callable=extract_from_mariadb
    )

    find_changed_cost_records_task = PythonOperator(
    task_id='find_changed_cost_records',
    python_callable=find_changed_cost_records
    )

    delete_changed_records_task = PythonOperator(
        task_id='delete_changed_records_from_clickhouse',
        python_callable=delete_changed_records_from_clickhouse
    )

    compare = PythonOperator(
        task_id='compare_data',
        python_callable=compare_data
    )

    load = PythonOperator(
        task_id='load_incremental_to_clickhouse',
        python_callable=load_incremental_to_clickhouse
    )
    
    cleanup = PythonOperator(
        task_id='cleanup_file',
        python_callable=cleanup_file,
        trigger_rule='none_failed_min_one_success'
    )
    
    # Логика выполнения
    check_volume >> extract >> find_changed_cost_records_task >> delete_changed_records_task >> compare >> load >> cleanup