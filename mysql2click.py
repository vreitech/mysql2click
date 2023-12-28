import sys
import signal
import time
import logging
import configparser
import asyncio
import aiomysql
import asynch

"""
Предлагаемая структура таблицы в ClickHouse:

CREATE TABLE `dc_log_old`
(
    `id` UInt64,
    `log_time` DateTime,
    `log_key` UInt32,
    `log_type` Int32,
    `log_data` String CODEC(ZSTD(14)),
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(log_time)
ORDER BY (log_type, log_key, id, log_time);
"""


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')
    conf_def = config["DEFAULT"]



    signal.signal(signal.SIGINT, self.default_int_handler)

    try:
        async def loop_mysql(loop):
            logger.info('Скрипт запущен')
            logger.info(f'🔧 БД источник (чтение)    : mysql://{conf_def["mysql_host_read"]}:{conf_def["mysql_port_read"]}/{conf_def["mysql_db"]}/{conf_def["mysql_table"]}')
            if conf_def.getboolean("make_mysql_delete"):
                logger.info(f'🔧 БД источник (удаление)  : mysql://{conf_def["mysql_host_delete"]}:{conf_def["mysql_port_delete"]}/{conf_def["mysql_db"]}/{conf_def["mysql_table"]}')
            logger.info(f'🔧 БД приёмник             : clickhouse://{conf_def["clickhouse_host"]}:{conf_def["clickhouse_port"]}/{conf_def["clickhouse_db"]}/{conf_def["clickhouse_table"]}')
            logger.info(f'🔧 Стартовая позиция id    : {conf_def["position_start"]}')
            logger.info(f'🔧 Конечная позиция id     : {conf_def["position_end"]}')
            logger.info(f'🔧 Размер пачки строк      : {conf_def["batch_rows"]}')
            logger.info(f'🔧 Пауза между пачками (с) : {conf_def["sleep_interval"]}')
            logger.warning('Ждём 10 секунд (последний шанс на отмену)...')
            await asyncio.sleep(10)

            position_current = conf_def.getint("position_start")

            logger.info('❕ Устанавливаем пул коннектов MySQL для чтения...')
            try:
                pool_mysql_read = await aiomysql.create_pool(
                    host = conf_def["mysql_host_read"],
                    port = conf_def.getint("mysql_port_read"),
                    user = conf_def["mysql_user_read"],
                    password = conf_def["mysql_password_read"],
                    db = conf_def["mysql_db"],
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

            if conf_def.getboolean("make_mysql_delete"):
                logger.info('❕ Устанавливаем пул коннектов MySQL для удаления...')
                try:
                    pool_mysql_delete = await aiomysql.create_pool(
                        host = conf_def["mysql_host_delete"],
                        port = conf_def.getint("mysql_port_delete"),
                        user = conf_def["mysql_user_delete"],
                        password = conf_def["mysql_password_delete"],
                        db = conf_def["mysql_db"],
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
                    host = conf_def["clickhouse_host"],
                    port = conf_def.getint("clickhouse_port"),
                    user = conf_def["clickhouse_user"],
                    password = conf_def["clickhouse_password"],
                    database = conf_def["clickhouse_db"]
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

                            await cur_mysql_read.execute("""SELECT `id`, `log_key`, `log_type`, `log_data`, `log_time` FROM `%s` WHERE `id` > %%s AND `id` <= %%s ORDER BY `id` LIMIT %%s""" % (conf_def["mysql_table"]), (position_current, conf_def.getint("position_end"), conf_def.getint("batch_rows"),))
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
                        if conf_def.getboolean("make_mysql_delete"):
                            logger.info('⏳ Удаление из таблицы в MySQL...')
                            conn_mysql_delete = await pool_mysql_delete.acquire()
                            cur_mysql_delete = await conn_mysql_delete.cursor()
                            try:
                                await cur_mysql_delete.execute("""DELETE FROM `%s` WHERE `id` > %%s AND `id` <= %%s""" % (conf_def["mysql_table"]), (position_current_at_read, position_current,))
                                row_delete = await cur_mysql_delete.fetchall()
                            except aiomysql.OperationalError as e:
                                logger.error('🛑 Ошибка при попытке выполнения запроса на удаление в MySQL!')
                                if e.args[0] == 1205:
                                    logger.warning(f'Произошла ошибка при попытке удаления записей с id в интервале с {position_current_at_read} по {position_current} из MySQL на хосте "{conf_def["mysql_host_delete"]}".')
                                    logger.warning('Работа скрипта будет продолжена, однако следует проверить, что в MySQL нет незакоммиченых XA-транзакций, блокирующих удаление.')
                                    logger.warning(f'Для проверки воспользуйтесь в клиенте MySQL командой "XA RECOVER;" на хосте "{conf_def["mysql_host_delete"]}".')
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
                        await asyncio.sleep(conf_def.getint("sleep_interval"))

            logger.info('❕ Закрываем пул коннектов MySQL для чтения...')
            pool_mysql_read.close()
            await pool_mysql_read.wait_closed()
            logger.info('✅ Успешно!')

            if conf_def.getboolean("make_mysql_delete"):
                logger.info('❕ Закрываем пул коннектов MySQL для удаления...')
                pool_mysql_delete.close()
                await pool_mysql_delete.wait_closed()
                logger.info('✅ Успешно!')

            if conf_def.getboolean("make_clickhouse_optimize"):
                logger.info('❕ Оптимизация целевой таблицы...')
                await asyncio.sleep(conf_def.getint("sleep_interval"))
                await optimize_clickhouse(pool_clickhouse)            
                logger.info('✅ Успешно!');

            logger.info('❕ Закрываем пул коннектов ClickHouse...')
            pool_clickhouse.close()
            await pool_clickhouse.wait_closed()
            logger.info('✅ Успешно!');
            
            logger.info('Скрипт завершён')

    except KeyboardInterrupt:
        logger.info('🔚 Получен сигнал на прерывание работы.')
        sys.exit(0)



    async def insert_clickhouse(pool_clickhouse, row):
        async with pool_clickhouse.acquire() as conn_clickhouse:
            async with conn_clickhouse.cursor(cursor=asynch.cursors.DictCursor) as cursor_clickhouse:
                rows_number = len(row)
                position_current = row[rows_number - 1][0];
                try:
                    ret_clickhouse = await cursor_clickhouse.execute("""
                        INSERT INTO `%s` (`id`, `log_key`, `log_type`, `log_data`, `log_time`) VALUES
                    """ % (conf_def["clickhouse_table"]), row)
                except:
                    logger.error('🛑 Ошибка при попытке выполнения запроса на вставку в ClickHouse!')
                    logger.exception(sys.exc_info()[0])
                    sys.exit(18)
                assert ret_clickhouse == rows_number, f'Ошибка при вставке в ClickHouse: несовпадение числа строк при вставке.\nПолучено {rows_number} строк из MySQL, но вставлено {ret_clickhouse} строк в ClickHouse.'
                logger.info(f'⏳ Строк вставлено    : {rows_number}')
                logger.info(f'⏳ Текущая позиция id : {position_current}')
                logger.info('⏳ Процент завершения : {:.2f}%'.format((position_current - conf_def.getint("position_start")) / (conf_def.getint("position_end") - conf_def.getint("position_start")) * 100))
                return position_current



    async def optimize_clickhouse(pool_clickhouse):
        async with pool_clickhouse.acquire() as conn_clickhouse:
            async with conn_clickhouse.cursor(cursor=asynch.cursors.DictCursor) as cursor_clickhouse:
                try:
                    ret_clickhouse = await cursor_clickhouse.execute("""OPTIMIZE TABLE `%s` DEDUPLICATE""" % (conf_def["clickhouse_table"]))
                except:
                    logger.error('🛑 Ошибка при попытке выполнения запроса на оптимизацию в ClickHouse!')
                    logger.exception(sys.exc_info()[0])
                    sys.exit(19)



    logger = logging.getLogger('aiomysql')
    logger.setLevel(logging.INFO)
    conhandler = logging.StreamHandler()
    conhandler.setLevel(logging.INFO)
    filehandler = logging.FileHandler(conf_def["log_file_name"], mode='a')
    filehandler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
    conhandler.setFormatter(formatter)
    filehandler.setFormatter(formatter)
    logger.addHandler(conhandler)
    logger.addHandler(filehandler)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop_mysql(loop))
