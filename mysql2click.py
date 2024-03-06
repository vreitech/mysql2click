import sys
import signal
import time
import logging
import argparse
import configparser
import asyncio
import aiomysql
import asynch

"""
Предлагаемая структура таблиц в ClickHouse:

CREATE TABLE dc_log_old
(
    `id` UInt64,
    `log_time` DateTime,
    `log_key` UInt32,
    `log_type` Int32,
    `log_data` String CODEC(ZSTD(14))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(log_time)
ORDER BY (log_type, log_key, id, log_time);

CREATE MATERIALIZED VIEW dc_log_old_max_id
(
    `log_date` Date CODEC(Delta, ZSTD(14)),
    `max_id` SimpleAggregateFunction(max, UInt64) CODEC(Delta, ZSTD(14))
)
ENGINE = AggregatingMergeTree
ORDER BY log_date
SETTINGS index_granularity = 8192 AS
SELECT
    DATE(log_time) AS log_date,
    max(id) AS max_id
FROM dc_log_old
GROUP BY log_date;

CREATE TABLE b_event_log_old
(
    `id` UInt64 CODEC(Delta, ZSTD(14)),
    `timestamp_x` DateTime CODEC(Delta, ZSTD(14)),
    `severity` LowCardinality(String) CODEC(ZSTD(14)),
    `audit_type_id` LowCardinality(String) CODEC(ZSTD(14)),
    `module_id` LowCardinality(String) CODEC(ZSTD(14)),
    `item_id` String CODEC(ZSTD(14)),
    `remote_addr` Nullable(String) CODEC(ZSTD(14)),
    `user_agent` Nullable(String) CODEC(ZSTD(14)),
    `request_uri` Nullable(String) CODEC(ZSTD(14)),
    `site_id` Nullable(FixedString(2)) CODEC(ZSTD(14)),
    `user_id` Nullable(UInt64) CODEC(ZSTD(14)),
    `guest_id` Nullable(UInt64) CODEC(ZSTD(14)),
    `description` Nullable(String) CODEC(ZSTD(14))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp_x)
ORDER BY (severity, audit_type_id, id, timestamp_x);

CREATE MATERIALIZED VIEW b_event_log_old_max_id
(
    `log_date` Date CODEC(Delta, ZSTD(14)),
    `max_id` SimpleAggregateFunction(max, UInt64) CODEC(Delta, ZSTD(14))
)
ENGINE = AggregatingMergeTree
ORDER BY log_date
SETTINGS index_granularity = 8192 AS
SELECT
    DATE(timestamp_x) AS log_date,
    max(id) AS max_id
FROM b_event_log_old
GROUP BY log_date;
"""



conf_full = configparser.ConfigParser()
conf_full.read('config.ini')
section = "DEFAULT"
args_parser = argparse.ArgumentParser(
    prog="mysql2click",
    description="Transferring data from a MySQL table to ClickHouse in chunks, operating the transfer range")
args_parser.add_argument('-s', '--section', dest='section')
args = args_parser.parse_args()
if (args.section is not None):
    section = args.section
conf = conf_full[section]

signal.signal(signal.SIGINT, self.sigIntTermHandler)
signal.signal(signal.SIGTERM, self.sigIntTermHandler)

async def loop_mysql(loop):
    logger.info('Скрипт запущен')
    logger.info(f'🔧 БД источник (чтение)    : mysql://{conf["mysql_host_read"]}:{conf["mysql_port_read"]}/{conf["mysql_db"]}/{conf["mysql_table"]}')
    if conf.getboolean("make_mysql_delete"):
        logger.info(f'🔧 БД источник (удаление)  : mysql://{conf["mysql_host_delete"]}:{conf["mysql_port_delete"]}/{conf["mysql_db"]}/{conf["mysql_table"]}')
    logger.info(f'🔧 БД приёмник             : clickhouse://{conf["clickhouse_host"]}:{conf["clickhouse_port"]}/{conf["clickhouse_db"]}/{conf["clickhouse_table"]}')
    logger.info(f'🔧 Стартовая позиция id    : {conf["position_start"]}')
    logger.info(f'🔧 Конечная позиция id     : {conf["position_end"]}')
    logger.info(f'🔧 Размер пачки строк      : {conf["batch_rows"]}')
    logger.info(f'🔧 Пауза между пачками (с) : {conf["sleep_interval"]}')
    logger.warning('Ждём 10 секунд (последний шанс на отмену)...')
    await asyncio.sleep(10)

    position_current = conf.getint("position_start")

    logger.info('❕ Устанавливаем пул коннектов MySQL для чтения...')
    try:
        pool_mysql_read = await aiomysql.create_pool(
            host = conf["mysql_host_read"],
            port = conf.getint("mysql_port_read"),
            user = conf["mysql_user_read"],
            password = conf["mysql_password_read"],
            db = conf["mysql_db"],
            minsize = 5,
            maxsize = 15,
            autocommit = True,
            echo = True
        )
    except:
        logger.error('🛑 Ошибка при попытке подключения к серверу MySQL!');
        logger.exception(sys.exc_info()[0])
        sys.exit(8)
    logger.info('✅ Успешно!')

    if conf.getboolean("make_mysql_delete"):
        logger.info('❕ Устанавливаем пул коннектов MySQL для удаления...')
        try:
            pool_mysql_delete = await aiomysql.create_pool(
                host = conf["mysql_host_delete"],
                port = conf.getint("mysql_port_delete"),
                user = conf["mysql_user_delete"],
                password = conf["mysql_password_delete"],
                db = conf["mysql_db"],
                minsize = 5,
                maxsize = 15,
                autocommit = True,
                echo = True
            )
        except:
            logger.error('🛑 Ошибка при попытке подключения к серверу MySQL!');
            logger.exception(sys.exc_info()[0])
            sys.exit(8)
        logger.info('✅ Успешно!')

    logger.info('❕ Устанавливаем пул коннектов ClickHouse...')
    try:
        pool_clickhouse = await asynch.create_pool(
            host = conf["clickhouse_host"],
            port = conf.getint("clickhouse_port"),
            user = conf["clickhouse_user"],
            password = conf["clickhouse_password"],
            database = conf["clickhouse_db"]
        )
    except:
        logger.error('🛑 Ошибка при попытке подключения к серверу ClickHouse!');
        logger.exception(sys.exc_info()[0])
        sys.exit(9)
    logger.info('✅ Успешно!')

    async with pool_mysql_read.acquire() as conn_mysql_read:
        async with conn_mysql_read.cursor() as cur_mysql_read:
            while True:
                logger.info('⏳ Чтение из MySQL...')
                try:
                    # Запоминаем позицию, при удалении нам она понадобится
                    position_current_at_read = position_current
                    
                    await cur_mysql_read.execute(conf["mysql_read_query"] % (conf["mysql_table"]), (position_current, conf.getint("position_end"), conf.getint("batch_rows"),))
                except:
                    logger.error('🛑 Ошибка при попытке выполнения запроса на чтение из MySQL!')
                    logger.exception(sys.exc_info()[0])
                    sys.exit(16)
                row = await cur_mysql_read.fetchall()
                if len(row) == 0:
                    logger.info('Новых строк в MySQL не найдено.')
                    break
                logger.info('⏳ Вставка в ClickHouse...')
                position_current = await insert_clickhouse(pool_clickhouse, row)
                if conf.getboolean("make_mysql_delete"):
                    logger.info('⏳ Удаление из таблицы в MySQL...')
                    conn_mysql_delete = await pool_mysql_delete.acquire()
                    cur_mysql_delete = await conn_mysql_delete.cursor()
                    try:
                        await cur_mysql_delete.execute(conf["mysql_delete_query"] % (conf["mysql_table"]), (position_current_at_read, position_current,))
                        row_delete = await cur_mysql_delete.fetchall()
                    except aiomysql.OperationalError as e:
                        logger.error('🛑 Ошибка при попытке выполнения запроса на удаление в MySQL!')
                        if e.args[0] == 1205:
                            logger.warning(f'Произошла ошибка при попытке удаления записей с id в интервале с {position_current_at_read} по {position_current} из MySQL на хосте "{conf["mysql_host_delete"]}".')
                            logger.warning('Работа скрипта будет продолжена, однако следует проверить, что в MySQL нет незакоммиченых XA-транзакций, блокирующих удаление.')
                            logger.warning(f'Для проверки воспользуйтесь в клиенте MySQL командой "XA RECOVER;" на хосте "{conf["mysql_host_delete"]}".')
                            logger.warning('Если команда вернёт одну или несколько записей - довольно велика вероятность того, что таблица в кластере MySQL неконсистентна')
                            logger.warning('как минимум по одному id из диапазона.')
                            logger.warning('В противном случае ошибка связана с чем-то другим и, возможно, имеет временный характер.')
                            logger.warning('Текст ошибки при попытке удаления представлен ниже.')
                            logger.exception(sys.exc_info()[0])
                        else:
                            logger.exception(sys.exc_info()[0])
                            sys.exit(17)
                    await cur_mysql_delete.close()
                    await pool_mysql_delete.release(conn_mysql_delete)
                await asyncio.sleep(conf.getint("sleep_interval"))

    logger.info('❕ Закрываем пул коннектов MySQL для чтения...')
    pool_mysql_read.close()
    await pool_mysql_read.wait_closed()
    logger.info('✅ Успешно!')

    if conf.getboolean("make_mysql_delete"):
        logger.info('❕ Закрываем пул коннектов MySQL для удаления...')
        pool_mysql_delete.close()
        await pool_mysql_delete.wait_closed()
        logger.info('✅ Успешно!')

    if conf.getboolean("make_clickhouse_optimize"):
        logger.info('❕ Оптимизация целевой таблицы...')
        await asyncio.sleep(conf.getint("sleep_interval"))
        await optimize_clickhouse(pool_clickhouse)            
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
            try:
                ret_clickhouse = await cursor_clickhouse.execute(conf["clickhouse_insert_query"] % (conf["clickhouse_table"]), row)
            except:
                logger.error('🛑 Ошибка при попытке выполнения запроса на вставку в ClickHouse!')
                logger.exception(sys.exc_info()[0])
                sys.exit(18)
            assert ret_clickhouse == rows_number, f'Ошибка при вставке в ClickHouse: несовпадение числа строк при вставке.\nПолучено {rows_number} строк из MySQL, но вставлено {ret_clickhouse} строк в ClickHouse.'
            logger.info(f'⏳ Строк вставлено    : {rows_number}')
            logger.info(f'⏳ Текущая позиция id : {position_current}')
            logger.info('⏳ Процент завершения : {:.2f}%'.format((position_current - conf.getint("position_start")) / (conf.getint("position_end") - conf.getint("position_start")) * 100))
            return position_current



async def optimize_clickhouse(pool_clickhouse):
    async with pool_clickhouse.acquire() as conn_clickhouse:
        async with conn_clickhouse.cursor(cursor=asynch.cursors.DictCursor) as cursor_clickhouse:
            try:
                ret_clickhouse = await cursor_clickhouse.execute(conf["clickhouse_optimize_query"] % (conf["clickhouse_table"]))
            except:
                logger.error('🛑 Ошибка при попытке выполнения запроса на оптимизацию в ClickHouse!')
                logger.exception(sys.exc_info()[0])
                sys.exit(19)



logger = logging.getLogger('aiomysql')
logger.setLevel(logging.INFO)
conhandler = logging.StreamHandler()
conhandler.setLevel(logging.INFO)
filehandler = logging.FileHandler(conf["log_file_name"], mode='a')
filehandler.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
conhandler.setFormatter(formatter)
filehandler.setFormatter(formatter)
logger.addHandler(conhandler)
logger.addHandler(filehandler)

loop = asyncio.get_event_loop()
loop.run_until_complete(loop_mysql(loop))

