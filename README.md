# mysql2click



## О репозитории

Назначение - копирование строк из таблицы в MySQL в таблицу в ClickHouse пачками в диапазоне от начального до конечного уникального идентификатора.

Параметры скрипта, такие как значения начального и конечного идентификатора, реквизиты подключения к БД, запросы на выборку и вставку, задаются в конфигурационном файле `config.ini`.

## Зависимости

Для работы требуются:
- (опционально) Docker либо Podman и Buildah
- Python3
- зависимости Python3 из `requirements.txt`

## Запуск

Создайте файл конфигурации `config.ini` и настройте его по аналогии с `config.ini.sample`.

- для запуска в Docker используйте скрипт `run_script.sh`
- для запуска в Podman измените в скрипте `run_script.sh` значение переменной `USE_DOCKER` на `n`, затем используйте скрипт `run_script.sh`
- для запуска без контейнеризации установите требуемые зависимости из `requrements.txt` самостоятельно, затем используйте команду `python3 ./mysql2click.py`

Если вы хотите запустить приложение не с конфигурацией из секции `[DEFAULT]`, а с другой секцией конфигурации из файла конфигурации `config.ini`, добавьте при запуске скрипта `run_script.sh` в качестве первого параметра имя секции конфигурации, например:
```
./run_script.sh another_one
```
В случае запуска без контейнеризации добавьте параметр `--section=<имя_секции>`, например:
```
python3 ./mysql2click.py --section=another_one
```

## Прочее

Скрипты `run_server.sh` и `run_client.sh` служат для запуска контейнеризированного сервера и клиента ClickHouse соответственно, добавлены в репозиторий во время и для целей отладки.
Скрипт `build_image.sh` служит для пересборки контейнера со скриптом, но без последующего запуска скрипта (в отличие от `run_script.sh`).
