# Импорты для работы с Apache Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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
import logging
import json

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
JOIN 
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
JOIN 
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
        
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_mysql_hook(self) -> MySqlHook:
        """Получение хука MySQL с повторными попытками"""
        if self._mysql_hook is None:
            logger.info("Creating MySQL hook connection")
            self._mysql_hook = MySqlHook(mysql_conn_id=MARIADB_CONN_ID)
            logger.info("MySQL hook created successfully")
        return self._mysql_hook
    
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_clickhouse_hook(self) -> ClickHouseHook:
        """Получение хука ClickHouse с повторными попытками"""
        if self._ch_hook is None:
            logger.info("Creating ClickHouse hook connection")
            self._ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
            # ПРОСТОЕ ТЕСТИРОВАНИЕ БЕЗ ПАРАМЕТРОВ
            client = self._ch_hook.get_conn()
            client.execute("SELECT 1")
            logger.info("ClickHouse connection tested successfully")
        return self._ch_hook
    
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_mysql_engine(self):
        """Получение SQLAlchemy engine с настройками для устойчивости"""
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
    
    def refresh_mysql_engine(self):
        """Принудительное обновление engine"""
        if self._mysql_engine:
            self._mysql_engine.dispose()
            self._mysql_engine = None
            logger.info("MySQL engine refreshed")
        return self.get_mysql_engine()
    
    def close_connections(self):
        """Закрытие всех соединений"""
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

# Мониторинг использования памяти для операции
@contextmanager
def memory_monitor(operation_name):
    start_memory = _get_memory_usage()
    start_time = time.time()
    
    try:
        yield
    finally:
        end_time = time.time()
        end_memory = _get_memory_usage()
        duration = end_time - start_time
        memory_diff = end_memory - start_memory
        
        logger.info(f"Memory monitor - {operation_name}: "
                   f"Time: {duration:.2f}s, "
                   f"Memory: +{memory_diff:.1f}MB "
                   f"({start_memory:.1f}MB -> {end_memory:.1f}MB)")
            
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

# Проверяем необходимость полной синхронизации
def check_full_resync_needed(**kwargs) -> bool:
    execution_date = kwargs['execution_date']
    dag_run = kwargs['dag_run']
    
    if execution_date.tzinfo is not None:
        naive_execution_date = execution_date.replace(tzinfo=None)
    else:
        naive_execution_date = execution_date
        
    if dag_run.conf and dag_run.conf.get('full_resync', False):
        logger.info("Full resync triggered by DAG parameters")
        return True
    
    if naive_execution_date.day == 1:
        logger.info("Full resync triggered - first day of month")
        return True
    
    if check_30_days_since_last_resync(execution_date):
        logger.info("Full resync triggered - more than 30 days since last full resync")
        return True
    
    logger.info("Incremental sync mode")
    return False

# Получаем дату последней полной синхронизации из Airflow Variables.
# Возвращаем дату 30 дней назад, если переменная не существует или повреждена.
def get_last_resync_date() -> datetime:
    try:
        # Пытаемся получить переменную
        last_resync_str = Variable.get("smokingshop_last_full_resync", default_var=None)
        
        # Если переменная не существует
        if last_resync_str is None:
            logger.info("Variable 'smokingshop_last_full_resync' does not exist")
            return _get_default_resync_date()
        
        # Если переменная пустая
        if not last_resync_str.strip():
            logger.warning("Variable 'smokingshop_last_full_resync' is empty")
            return _get_default_resync_date()
        
        # Парсим JSON
        last_resync_data = json.loads(last_resync_str)
        
        # Проверяем наличие обязательного поля
        if 'last_resync_date' not in last_resync_data:
            logger.warning("Missing 'last_resync_date' key in resync data")
            return _get_default_resync_date()
        
        # Парсим дату
        last_resync_date = datetime.fromisoformat(last_resync_data['last_resync_date'])
        
        # Убираем timezone info если есть
        if last_resync_date.tzinfo is not None:
            last_resync_date = last_resync_date.replace(tzinfo=None)
                
        logger.info(f"Last full resync was on: {last_resync_date}")
        return last_resync_date
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in resync variable: {e}")
    except ValueError as e:
        logger.error(f"Invalid date format in resync variable: {e}")
    except Exception as e:
        logger.error(f"Unexpected error reading resync date: {e}")
    
    # Fallback на дату по умолчанию при любой ошибке
    return _get_default_resync_date()

# Возвращаем дату по умолчанию (30 дней назад)
def _get_default_resync_date() -> datetime:
    default_date = datetime.now(timezone.utc) - timedelta(days=30)
    if default_date.tzinfo is not None:
        default_date = default_date.replace(tzinfo=None)
    logger.info(f"Using default resync date: {default_date}")
    return default_date

# Обновляем дату последней полной синхронизации в Airflow Variables
def update_last_resync_date(execution_date: datetime):
    try:
        if execution_date.tzinfo is not None:
            naive_execution_date = execution_date.replace(tzinfo=None)
        else:
            naive_execution_date = execution_date
            
        resync_data = {
            'last_resync_date': naive_execution_date.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        Variable.set("smokingshop_last_full_resync", json.dumps(resync_data))
        logger.info(f"Updated last resync date to: {naive_execution_date}")
    except Exception as e:
        logger.error(f"Error updating last resync date: {e}")

# Проверяем, прошло ли 30 дней с момента последней полной синхронизации
def check_30_days_since_last_resync(execution_date: datetime) -> bool:
    last_resync_date = get_last_resync_date()
    
    if execution_date.tzinfo is not None:
        naive_execution_date = execution_date.replace(tzinfo=None)
    else:
        naive_execution_date = execution_date
    
    days_since_last_resync = (naive_execution_date - last_resync_date).days
    
    logger.info(f"Days since last full resync: {days_since_last_resync}")
    
    if days_since_last_resync >= 30:
        logger.info(f"Triggering full resync - {days_since_last_resync} days since last resync")
        return True
    
    return False

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

# Основные функции ETL с улучшенной обработкой ошибок
# Полная синхронизация. Замена всех данных в целевой таблице на все данные из рецепиента
@retry_with_backoff(max_retries=2, retry_delay=30)  # Меньше повторных попыток для полной синхронизации
def full_resync(**kwargs):
    import gc
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        temp_table_name = None
        
        # Шаг 1: Получаем информацию об объеме данных и задаём chunk_size
        volume_info = ti.xcom_pull(task_ids='check_data_volume', key='data_volume_info')
        
        if volume_info:
            total_count = volume_info.get('total_count', 0)
            warning_level = volume_info.get('warning_level', 'NORMAL')
            
            if warning_level == "CRITICAL":
                chunk_size = 25000  # Уменьшаем для очень больших датасетов
            elif warning_level == "WARNING":
                chunk_size = 50000
            else:
                chunk_size = 100000
        else:
            chunk_size = 100000
        logger.info(f"Using chunk size {chunk_size}")

        with DatabaseConnectionContext() as db:
            # Проверяем подключения
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            engine = db.get_mysql_engine()

            # Шаг 2: Создаем временную таблицу вместо прямого удаления
            temp_table_name = f"{TABLE_NAME}_temp_{int(time.time())}"

            create_temp_table_sql = f"""
            CREATE TABLE {temp_table_name} (
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
            logger.info(f"Creating temporary table: {temp_table_name}")
            client.execute(create_temp_table_sql)

            # SQL вставки для временной таблицы
            insert_sql = f"""
                INSERT INTO {temp_table_name} (
                    tabular_row_id, sbis_date, number, document_id, sbis_account_id, 
                    sbis_account_name, is_main, status, nomenclature_code, nomenclature_title, 
                    nomenclature_count, nomenclature_price, nomenclature_cost_price,
                    nomenclature_price_total, nomenclature_cost_price_total,
                    realization_url, client_id, employee_id
                ) VALUES
            """

            total_rows = 0
            chunk_number = 0
            memory_stats = []
            # Используем внешний словарь для отслеживания повторных попыток
            chunk_retries = {}

            # Шаг 3: Вставляем данные во временную таблицу с улучшенной обработкой ошибок
            for chunk_df in pd.read_sql(SQL_QUERY_ALL, con=engine, chunksize=chunk_size):
                chunk_number += 1
                
                try:
                    # Проверяем соединение перед обработкой чанка
                    with engine.connect() as test_conn:
                        test_conn.execute("SELECT 1")
                    
                    logger.info(f"Processing chunk {chunk_number} with {len(chunk_df)} rows")

                    with memory_monitor("Process chunk"):
                        # Преобразуем типы
                        chunk_df = convert_data_types(chunk_df)

                        # Подготавливаем данные для вставки
                        data = prepare_data_for_insert(chunk_df)

                    # ОПТИМИЗАЦИЯ ПАМЯТИ: Явно удаляем DataFrame
                    del chunk_df

                    # Вставляем во временную таблицу
                    client.execute(insert_sql, data)
                    total_rows += len(data)

                    # ОПТИМИЗАЦИЯ ПАМЯТИ: Освобождаем список данных
                    del data
                    
                    # ОПТИМИЗАЦИЯ ПАМЯТИ: Периодическая сборка мусора
                    if chunk_number % 5 == 0:
                        gc.collect()
                        # Логируем использование памяти
                        memory_usage = _get_memory_usage()
                        memory_stats.append(memory_usage)
                        logger.info(f"Chunk {chunk_number}: {total_rows} total rows, Memory: {memory_usage} MB")

                    # Логируем прогресс каждые 10 чанков
                    if chunk_number % 10 == 0:
                        logger.info(f"Progress: processed {chunk_number} chunks, {total_rows} total rows")
                        
                except (OperationalError, DatabaseError) as e:
                    max_chunk_retries = 3
                    chunk_key = f"chunk_{chunk_number}"
                    current_retry_count = chunk_retries.get(chunk_key, 0)
                    
                    if current_retry_count >= max_chunk_retries:
                        logger.error(f"Max retries exceeded for chunk {chunk_number}")
                        raise
                    
                    logger.warning(f"Database error in chunk {chunk_number} (retry {current_retry_count + 1}/{max_chunk_retries}): {str(e)}")

                    # Обновляем счетчик
                    chunk_retries[chunk_key] = current_retry_count + 1
                    
                    # ОПТИМИЗАЦИЯ ПАМЯТИ: Освобождаем память перед повторной попыткой
                    if 'data' in locals():
                        del data
                    if 'chunk_df' in locals():
                        del chunk_df
                    gc.collect()

                    # Обновляем соединения
                    db.close_connections()
                    engine = db.get_mysql_engine()
                    client = db.get_clickhouse_hook().get_conn()
                    
                    # Устанавливаем счетчик повторных попыток
                    if hasattr(chunk_df, '_retry_count'):
                        chunk_df._retry_count += 1
                    else:
                        chunk_df._retry_count = 1
                    
                    chunk_number -= 1  # Важно: уменьшаем счетчик для повторной обработки того же чанка
                    continue
                    
                except Exception as e:
                    logger.error(f"Unexpected error in chunk {chunk_number}: {str(e)}")
                    # При критической ошибке удаляем временную таблицу
                    try:
                        client.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                        logger.info(f"Temporary table {temp_table_name} dropped due to error")
                    except Exception as drop_error:
                        logger.warning(f"Could not drop temporary table: {drop_error}")
                    raise

            # Финальная сборка мусора
            gc.collect()

            logger.info(f"Data insertion completed. Total rows inserted into temporary table: {total_rows}")

            # Логируем статистику памяти
            if memory_stats:
                avg_memory = sum(memory_stats) / len(memory_stats)
                max_memory = max(memory_stats)
                logger.info(f"Memory usage - Avg: {avg_memory:.1f} MB, Max: {max_memory:.1f} MB")

            # Шаг 4: Атомарная замена таблиц
            logger.info("Performing atomic table swap...")
            
            # Проверяем, существует ли оригинальная таблица
            check_table_sql = f"""
            SELECT COUNT() as count FROM system.tables 
            WHERE database = currentDatabase() AND name = '{TABLE_NAME}'
            """
            table_exists = client.execute(check_table_sql)[0][0] > 0
            
            if table_exists:
                # Переименовываем старую таблицу для бэкапа
                backup_table_name = f"{TABLE_NAME}_backup_{int(time.time())}"
                client.execute(f"RENAME TABLE {TABLE_NAME} TO {backup_table_name}")
                logger.info(f"Original table renamed to backup: {backup_table_name}")
            
            # Переименовываем временную таблицу в основную
            client.execute(f"RENAME TABLE {temp_table_name} TO {TABLE_NAME}")
            logger.info(f"Temporary table renamed to main: {TABLE_NAME}")
            
            # Удаляем бэкап, если он есть
            if table_exists:
                client.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
                logger.info(f"Backup table {backup_table_name} dropped")

        update_last_resync_date(execution_date)
        logger.info(f"Full resync completed successfully. Total rows inserted: {total_rows}")

    except Exception as e:
        logger.error(f"Error in full_resync: {str(e)}", exc_info=True)
        
        # При ошибке пытаемся очистить временные таблицы
        try:
            if temp_table_name:  # Проверяем, что переменная определена
                with DatabaseConnectionContext() as db:
                    ch_hook = db.get_clickhouse_hook()
                    client = ch_hook.get_conn()
                    client.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                    logger.info(f"Temporary table {temp_table_name} cleaned up after error")
        except Exception as cleanup_error:
            logger.warning(f"Could not clean up temporary table: {cleanup_error}")
            
        raise AirflowException(f"Full resync failed: {str(e)}")

#  Получить текущее использование памяти в MB
def _get_memory_usage():
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024  # в MB
    except ImportError:
        try:
            import resource
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # в MB
        except:
            return 0

# Получение данных из MariaDB
@retry_with_backoff()
def extract_from_mariadb(**kwargs):
    import gc
    try:
        max_retries=MAX_RETRIES
        export_path = EXPORT_PATH
        ti = kwargs['ti']
#        full_resync_mode = ti.xcom_pull(key='full_resync_needed', task_ids='check_sync_mode')
        
        logger.info("Starting data extraction from MariaDB")

        with DatabaseConnectionContext() as db:
            engine = db.get_mysql_engine()

            volume_info = ti.xcom_pull(task_ids='check_data_volume', key='data_volume_info')
        
            if volume_info:
                total_count = volume_info.get('total_count')
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
#                    continue
                    
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
                csv_path = export_path + '.csv'
                df_mariadb.to_csv(csv_path, index=False)
                export_path = csv_path  # Меняем локальную переменную, а не глобальную
                logger.info(f"Data saved to CSV instead: {export_path}")
            
            # Формируем DataFrame с одним столбцом 'key'
            df_keys = pd.DataFrame({'key': all_keys})

            # Путь для сохранения ключей — можно сделать рядом с данными из export_path
            keys_path_parquet = export_path.replace('.parquet', '_keys.parquet')
            keys_path_csv = export_path.replace('.parquet', '_keys.csv')

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

# Сравнение по ключам данных из Maria DB и Clickhouse: появились ли новые записи для вставки
@retry_with_backoff()
def compare_data(**kwargs):
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        mariadb_data_path = ti.xcom_pull(key='mariadb_data', task_ids='extract_from_mariadb')
        keys_path = ti.xcom_pull(task_ids='extract_from_mariadb', key='mariadb_keys_path')
        new_keys_path =  f'/tmp/new_keys_{execution_date.strftime("%Y%m%d_%H%M%S")}.parquet'
        keys_temp_path = f'/tmp/new_records_all_{execution_date.strftime("%Y%m%d_%H%M%S")}.parquet'
        
        if keys_path.endswith('.parquet'):
            df_keys = pd.read_parquet(keys_path)
        else:
            df_keys = pd.read_csv(keys_path)

        if df_keys.empty or 'key' not in df_keys.columns:
            logger.warning("Keys file is empty or invalid.")
            ti.xcom_push(key='new_records_keys_path', value=None)
            ti.xcom_push(key='has_new_records', value=False)
            return
        
        mariadb_keys = df_keys['key'].tolist()
        
        if not mariadb_data_path or not mariadb_keys:
            logger.info("No data from MariaDB, skipping comparison")
            ti.xcom_push(key='new_records_keys_path', value=None)
            ti.xcom_push(key='has_new_records', value=False)
            return
        
        logger.info("Starting data comparison between MariaDB and ClickHouse")

        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            
            check_table_sql = f"""
            SELECT COUNT() FROM system.tables 
            WHERE database = currentDatabase() AND name = '{TABLE_NAME}'
            """
            table_exists = client.execute(check_table_sql)[0][0] > 0
            
            if not table_exists:
                logger.info(f"Table {TABLE_NAME} does not exist in ClickHouse. All records are new.")
                pd.DataFrame({'key': mariadb_keys}).to_parquet(keys_temp_path, index=False)
                ti.xcom_push(key='new_records_keys_path', value=keys_temp_path)
                ti.xcom_push(key='has_new_records', value=True)
                ti.xcom_push(key='new_records_count', value=len(mariadb_keys))
                return
            
            logger.info("Getting existing keys from ClickHouse")
            ch_keys_data = client.execute(f"""
                SELECT sbis_account_id, document_id, tabular_row_id FROM {TABLE_NAME}
            """)
            
            if not ch_keys_data:
                logger.info("No existing data in ClickHouse. All records are new.")
                pd.DataFrame({'key': mariadb_keys}).to_parquet(keys_temp_path, index=False)
                ti.xcom_push(key='new_records_keys_path', value=keys_temp_path)
                ti.xcom_push(key='has_new_records', value=True)
                ti.xcom_push(key='new_records_count', value=len(mariadb_keys))
                return
            
            ch_keys = [f"{row[0]}_{row[1]}_{row[2]}" for row in ch_keys_data]            
            logger.info(f"Found {len(ch_keys)} existing records in ClickHouse")
            
            new_keys = set(mariadb_keys) - set(ch_keys)
            logger.info(f"Comparison completed. New records found: {len(new_keys)}")
            
            if not new_keys:
                logger.info("No new records found. DAG will complete.")
                ti.xcom_push(key='new_records_keys_path', value=None)
                ti.xcom_push(key='has_new_records', value=False)
                ti.xcom_push(key='new_records_count', value=0)
            else:
                pd.DataFrame({'key': list(new_keys)}).to_parquet(new_keys_path, index=False)
                ti.xcom_push(key='new_records_keys_path', value=new_keys_path)
                ti.xcom_push(key='has_new_records', value=True)
                ti.xcom_push(key='new_records_count', value=len(new_keys))
        
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

    except Exception as e:
        logger.error(f"Error in cleanup_file: {str(e)}", exc_info=True)

# Проверка: какой из вариантов синхронизации требуется: полная, или инкрементальная
def check_sync_mode(**kwargs):
    try:
        full_resync_needed = check_full_resync_needed(**kwargs)
        ti = kwargs['ti']
        ti.xcom_push(key='full_resync_needed', value=full_resync_needed)
        
        if full_resync_needed:
            logger.info("SYNC MODE: FULL RESYNC")
        else:
            logger.info("SYNC MODE: INCREMENTAL")
            
    except Exception as e:
        logger.error(f"Error in check_sync_mode: {str(e)}")
        raise

# Ветвление: полная синхронизация, или инкрементальная
def decide_sync_branch(**kwargs):
    try:
        ti = kwargs['ti']
        full_resync_needed = ti.xcom_pull(key='full_resync_needed', task_ids='check_sync_mode')
        
        if full_resync_needed is None:
            logger.warning("No sync mode found, defaulting to incremental")
            return 'extract_from_mariadb'
        
        if full_resync_needed:
            logger.info("Branch selected: FULL RESYNC")
            return 'check_data_volume'
        else:
            logger.info("Branch selected: INCREMENTAL SYNC")
            return 'extract_from_mariadb'
    except Exception as e:
        logger.error(f"Error in branch decision: {e}")
        return 'extract_from_mariadb'

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
    dag_id='smokingshop_mariadb_to_clickhouse_improved',
    start_date=datetime(2024, 10, 1, 5, 15),
    schedule='15 5,7,9,11,13 * * *',
    catchup=False,
    tags=['smokingshop','mariadb', 'clickhouse', 'etl', 'incremental', 'improved'],
    max_active_runs=1,
    concurrency=1,
    default_args=default_args
) as dag:
    
    check_mode = PythonOperator(
        task_id='check_sync_mode',
        python_callable=check_sync_mode
    )

    check_volume = PythonOperator(
    task_id='check_data_volume',
    python_callable=check_data_volume,
    provide_context=True
    )
        
    branch_operator = BranchPythonOperator(
        task_id='decide_sync_branch',
        python_callable=decide_sync_branch
    )
    
    full_resync_task = PythonOperator(
        task_id='full_resync',
        python_callable=full_resync
    )
    
    extract = PythonOperator(
        task_id='extract_from_mariadb',
        python_callable=extract_from_mariadb
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
    check_mode >> branch_operator
    branch_operator >> check_volume        # Только для полной синхронизации
    branch_operator >> extract             # Для инкрементальной синхронизации
    check_volume >> full_resync_task       # После проверки объема -> полная синхронизация
    extract >> compare >> load
    full_resync_task >> cleanup
    load >> cleanup