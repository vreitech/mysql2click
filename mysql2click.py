import asyncio
import aiomysql
import asynch
import logging

"""
Предлагаемая структура таблицы в ClickHouse:

CREATE TABLE `dc_log_old`
(
    `id` UInt64,
    `log_key` UInt32,
    `log_type` Int32,
    `log_data` String CODEC(ZSTD(16)),
    `log_time` DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(log_time)
PRIMARY KEY id
ORDER BY (id, log_time);
"""

log_file_name = "./mysql2click.log"
mysql_host = "127.0.0.1"
mysql_port = 3306
mysql_user = "root"
mysql_password = "<MYSQL_PASSWORD_HERE>"
mysql_db = "dc_bitrix"
clickhouse_host = "127.0.0.1"
clickhouse_port = 9000
clickhouse_user = "root"
clickhouse_password = "<CLICKHOUSE_PASSWORD_HERE>"
clickhouse_db = "default"

mysql_table = 'dc_log' # Имя таблицы в исходной БД
clickhouse_table = 'dc_log_old' # Имя таблицы в целевой БД
position_start = 27756000000
position_end =   27756100000
batch_rows = 10000 # Размер пачки строк - единицы перемещения из одной БД в другую в данном скрипте.
make_mysql_deletes = False # Производить удаления из БД источника после переноса. `mysql_host` и `mysql_port` должны указывать на мастер!
sleep_interval = 2 # На сколько секунд засыпать после переноса каждой пачки строк. Увеличьте, если слишком высокая нагрузка, в особенности если используете `make_mysql_deletes`



async def loop_mysql(loop):
    logger.info('Скрипт запущен')
    logger.info(f'🔧 БД источник            : mysql://{mysql_host}:{mysql_port}/{mysql_db}/{mysql_table}')
    logger.info(f'🔧 БД приёмник            : clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}/{clickhouse_table}')
    logger.info(f'🔧 Размер пачки строк     : {batch_rows}')
    logger.info(f'🔧 Стартовая позиция id   : {position_start}')
    logger.info(f'🔧 Конечная позиция id    : {position_end}')
    logger.info(f'🔧 Интервал между пачками : {sleep_interval}')
    position_current = position_start
    logger.info('❕ Устанавливаем пул коннектов MySQL...')
    try:
        pool_mysql = await aiomysql.create_pool(
            host = mysql_host,
            port = mysql_port,
            user = mysql_user,
            password = mysql_password,
            db = mysql_db
        )
    except:
        logger.info('🛑 Ошибка при попытке подключения к серверу MySQL!');
        raise
    logger.info('✅ Успешно!')

    logger.info('❕ Устанавливаем пул коннектов ClickHouse...')
    try:
        pool_clickhouse = await asynch.create_pool(
            host = clickhouse_host,
            port = clickhouse_port,
            user = clickhouse_user,
            password = clickhouse_password,
            database = clickhouse_db
        )
    except:
        logger.info('🛑 Ошибка при попытке подключения к серверу ClickHouse!');
        raise
    logger.info('✅ Успешно!')

    async with pool_mysql.acquire() as conn_mysql:
        async with conn_mysql.cursor() as cur_mysql:
            while True:
                await cur_mysql.execute("""
                    SELECT `id`, `log_key`, `log_type`, `log_data`, `log_time` FROM `%s` WHERE `id` > %%s and `id` <= %%s ORDER BY `id` LIMIT %%s
                """ % (mysql_table), (position_current, position_end, batch_rows,))
                row = await cur_mysql.fetchall()
                if len(row) == 0:
                    break
                position_current = await insert_clickhouse(pool_clickhouse, row)
                await asyncio.sleep(sleep_interval)

    logger.info('❕ Закрываем пул коннектов MySQL...')
    pool_mysql.close()
    await pool_mysql.wait_closed()
    logger.info('✅ Успешно!')

    await asyncio.sleep(sleep_interval)
    logger.info('❕ Оптимизация целевой таблицы...')
    async with pool_clickhouse.acquire() as conn_clickhouse:
        async with conn_clickhouse.cursor(cursor=asynch.cursors.DictCursor) as cursor_clickhouse:
            ret_clickhouse = await cursor_clickhouse.execute("""
                OPTIMIZE TABLE `%s`
            """ % (clickhouse_table))
            
    logger.info('✅ Успешно!');

    logger.info('❕ Закрываем пул коннектов ClickHouse...')
    pool_clickhouse.close()
    await pool_clickhouse.wait_closed()
    logger.info('✅ Успешно!');
    
    logger.info('Скрипт завершён')



async def insert_clickhouse(pool_clickhouse, row):
    async with pool_clickhouse.acquire() as conn_clickhouse:
        async with conn_clickhouse.cursor(cursor=asynch.cursors.DictCursor) as cursor_clickhouse:
            rows_number = len(row)
            position_current = row[rows_number - 1][0];
            ret_clickhouse = await cursor_clickhouse.execute("""
                INSERT INTO `%s` (`id`, `log_key`, `log_type`, `log_data`, `log_time`) VALUES
            """ % (clickhouse_table), row
            )
            assert ret_clickhouse == rows_number, f'Ошибка при вставке в ClickHouse: несовпадение числа строк при вставке.\nПолучено {rows_number} строк из MySQL, но вставлено {ret_clickhouse} строк в ClickHouse.'
            logger.info(f'⏳ Строк вставлено    : {rows_number}')
            logger.info(f'⏳ Текущая позиция id : {position_current}')
            logger.info('⏳ Процент завершения : {:.2f}%'.format((position_current - position_start) / (position_end - position_start) * 100))
            return position_current



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
conhandler = logging.StreamHandler()
conhandler.setLevel(logging.INFO)
filehandler = logging.FileHandler(log_file_name, mode='a')
filehandler.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
conhandler.setFormatter(formatter)
filehandler.setFormatter(formatter)
logger.addHandler(conhandler)
logger.addHandler(filehandler)

loop = asyncio.get_event_loop()
loop.run_until_complete(loop_mysql(loop))
