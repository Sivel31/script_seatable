#####################################################################################
# Скрипт получает список СПО из VictoriaMetrics ЦВПЗ и их версий                    #
# Загружает в таблицу "Версии СПО"                                                  #
#####################################################################################

import requests
import os
import urllib3
from datetime import datetime
from seatable_api import Base
import logging
from logging.handlers import RotatingFileHandler


# Настройка модуля logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler(
            "software_versions_update.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
    force=True,
)

# Создаем логгер для текущего модуля
logger = logging.getLogger(__name__)


# Отключаем SSL-предупреждения
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Конфигурация VictoriaMetrics
VMSELECT_URL = "https://cvpz-user-vmauth.data.corp/api/v1/query"
TOKEN = os.getenv("VICTORIA_TOKEN")
if not TOKEN:
    logger.error("Environment variable VICTORIA_TOKEN is not set")
    raise ValueError("Environment variable VICTORIA_TOKEN is not set")
TIMEOUT = 10

# ID аккаунта и проектов в VictoriaMetrics
VM_ACCOUNT_ID = "2"
VM_PROJECT_IDS = ["1", "2", "3", "4"]

# Заголовки для запросов к VictoriaMetrics
HEADERS = {"Authorization": f"Bearer {TOKEN}", "TenantID": VM_ACCOUNT_ID}


# Конфигурация SeaTable
SEATABLE_TOKEN = os.getenv("API_SEATABLE_MDM")
if not SEATABLE_TOKEN:
    logger.error("Environment variable API_SEATABLE_MDM is not set")
    raise ValueError("Environment variable API_SEATABLE_MDM is not set")

SERVER_URL = os.getenv("SEATABLE_SERVER")
if not SERVER_URL:
    logger.error("Environment variable SEATABLE_SERVER is not set")
    raise ValueError("Environment variable SEATABLE_SERVER is not set")

TABLE_NAME = "Версии СПО"

# Создаем подключение к SeaTable
base = Base(SEATABLE_TOKEN, SERVER_URL)
base.auth()


def fetch_metric(query):
    """Выполняет запрос к VictoriaMetrics и возвращает результат метрики."""
    try:
        response = requests.get(
            VMSELECT_URL,
            params={"query": query},
            headers=HEADERS,
            timeout=TIMEOUT,
            verify=False,
        )
        response.raise_for_status()
        data = response.json()
        return data["data"]["result"] if data["status"] == "success" else []
    except Exception as e:
        logger.error(f"Ошибка запроса: {e}")
        return []


def get_all_software_versions(time_range="3h"):
    """Получает все версии ПО со всех хостов из различных источников."""
    software_versions = {}

    # 1. Основной запрос для версий ПО из script_exporter
    for project_id in VM_PROJECT_IDS:
        main_query = f'last_over_time({{job="script_exporter_sys_app_versions", __name__!~"script_success|up|zabbix_agent_version|filebeat_version|consulmdm_version", vm_project_id="{project_id}"}}[{time_range}])'

        logger.debug(f"Основной запрос для project_id={project_id}")
        main_result = fetch_metric(main_query)

        for item in main_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            metric_name = labels.get("__name__", "")
            mdm_env = labels.get("mdm_env", "")

            # Определяем имя ПО из __name__
            software_name = metric_name
            if software_name.endswith("_version"):
                software_name = software_name[:-8]  # Убираем "_version"
            if software_name.endswith("mdm"):
                software_name = software_name[:-3]  # Убираем "mdm"

            # Преобразуем имя ПО в читаемый формат (первая буква заглавная)
            if software_name:
                software_name = software_name.capitalize()

            # Получаем версию из меток
            version = labels.get("version")
            if not version:
                version = labels.get("client_version")
            if not version:
                version = labels.get("server_version")

            if hostname and software_name and version:
                normalized_hostname = hostname.lower().strip()
                key = f"{normalized_hostname}::{software_name}"

                software_versions[key] = {
                    "host": normalized_hostname,
                    "software": software_name,
                    "version": version,
                    "contour": mdm_env if mdm_env else "не указан",
                }

    # 2. Запрос для PostgreSQL (pg_static)
    for project_id in VM_PROJECT_IDS:
        pg_query = f'last_over_time({{__name__="pg_static", vm_project_id="{project_id}"}}[{time_range}])'

        logger.debug(f"PostgreSQL запрос для project_id={project_id}")
        pg_result = fetch_metric(pg_query)

        for item in pg_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            mdm_env = labels.get("mdm_env", "")
            version = labels.get("short_version")

            if hostname and version:
                normalized_hostname = hostname.lower().strip()
                software_name = "PostgreSQL"
                key = f"{normalized_hostname}::{software_name}"

                # Добавляем только если такой записи еще нет
                if key not in software_versions:
                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software_name,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан",
                    }

    # 3. Запрос для RabbitMQ (rabbitmq_version_info)
    for project_id in VM_PROJECT_IDS:
        rabbitmq_query = f'last_over_time({{__name__="rabbitmq_version_info", vm_project_id="{project_id}"}}[{time_range}])'

        logger.debug(f"RabbitMQ запрос для project_id={project_id}")
        rabbitmq_result = fetch_metric(rabbitmq_query)

        for item in rabbitmq_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            mdm_env = labels.get("mdm_env", "")
            version = labels.get("rabbitmq")

            if hostname and version:
                normalized_hostname = hostname.lower().strip()
                software_name = "RabbitMQ"
                key = f"{normalized_hostname}::{software_name}"

                # Добавляем только если такой записи еще нет
                if key not in software_versions:
                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software_name,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан",
                    }

    # 4. Запрос для Redis (redis_instance_info)
    for project_id in VM_PROJECT_IDS:
        redis_query = f'last_over_time({{__name__="redis_instance_info", vm_project_id="{project_id}"}}[{time_range}])'

        logger.debug(f"Redis запрос для project_id={project_id}")
        redis_result = fetch_metric(redis_query)

        for item in redis_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            mdm_env = labels.get("mdm_env", "")
            version = labels.get("redis_version")

            if hostname and version:
                normalized_hostname = hostname.lower().strip()
                software_name = "Redis"
                key = f"{normalized_hostname}::{software_name}"

                # Добавляем только если такой записи еще нет
                if key not in software_versions:
                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software_name,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан",
                    }

    # 5. Запрос для Elasticsearch (elasticsearch_clusterinfo_version_info)
    for project_id in VM_PROJECT_IDS:
        es_query = f'last_over_time({{__name__="elasticsearch_clusterinfo_version_info", vm_project_id="{project_id}"}}[{time_range}])'

        logger.debug(f"Elasticsearch запрос для project_id={project_id}")
        es_result = fetch_metric(es_query)

        for item in es_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            mdm_env = labels.get("mdm_env", "")
            version = labels.get("version")  # Метка version содержит версию

            if hostname and version:
                normalized_hostname = hostname.lower().strip()

                # Определяем имя ПО на основе версии
                try:
                    version_parts = version.split(".")
                    major_version = int(version_parts[0]) if version_parts else 0

                    # Если версия меньше 5 - это Opensearch (ES 1.x, 2.x), иначе Elasticsearch
                    if major_version < 5:
                        software_name = "Opensearch"
                    else:
                        software_name = "Elasticsearch"

                    logger.debug(
                        f"ES версия {version} -> {software_name} (мажорная: {major_version})"
                    )

                except (ValueError, AttributeError, IndexError):
                    # Если не удалось распарсить версию, используем по умолчанию
                    software_name = "Elasticsearch"
                    logger.debug(
                        f"Не удалось распарсить версию ES: {version}, используем по умолчанию {software_name}"
                    )

                key = f"{normalized_hostname}::{software_name}"

                # Добавляем только если такой записи еще нет
                if key not in software_versions:
                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software_name,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан",
                    }

    # 6. Запрос для ETCD (etcd_server_version)
    for project_id in VM_PROJECT_IDS:
        etcd_query = f'last_over_time({{__name__="etcd_server_version", vm_project_id="{project_id}"}}[{time_range}])'

        logger.debug(f"ETCD запрос для project_id={project_id}")
        etcd_result = fetch_metric(etcd_query)

        for item in etcd_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            mdm_env = labels.get("mdm_env", "")
            version = labels.get(
                "server_version"
            )  # Метка server_version содержит версию

            if hostname and version:
                normalized_hostname = hostname.lower().strip()
                software_name = "ETCD"
                key = f"{normalized_hostname}::{software_name}"

                # Добавляем только если такой записи еще нет
                if key not in software_versions:
                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software_name,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан",
                    }

    # 7. Запрос для vmagent (vm_app_version)
    for project_id in VM_PROJECT_IDS:
        vmagent_query = f'last_over_time({{__name__="vm_app_version", vm_project_id="{project_id}"}}[{time_range}])'

        logger.debug(f"vmagent запрос для project_id={project_id}")
        vmagent_result = fetch_metric(vmagent_query)

        for item in vmagent_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            mdm_env = labels.get("mdm_env", "")
            version = labels.get("short_version")  # Метка short_version содержит версию

            if hostname and version:
                normalized_hostname = hostname.lower().strip()
                software_name = "vmagent"
                key = f"{normalized_hostname}::{software_name}"

                # Добавляем только если такой записи еще нет
                if key not in software_versions:
                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software_name,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан",
                    }

    # Статистика
    software_stats = {}
    for data in software_versions.values():
        software = data["software"]
        software_stats[software] = software_stats.get(software, 0) + 1

    logger.info(
        f"Получено данных о версиях ПО для {len(set(v['host'] for v in software_versions.values()))} хостов"
    )
    logger.info(f"Всего уникальных записей ПО: {len(software_versions)}")

    for software, count in sorted(software_stats.items()):
        logger.info(f"  {software}: {count} записей")

    return software_versions


def sync_versions_to_seatable():
    """Синхронизирует версии ПО из VictoriaMetrics в таблицу SeaTable."""

    # Получаем существующие строки из таблицы
    try:
        rows = base.list_rows(TABLE_NAME)
        logger.info(f"Загружено {len(rows)} строк из таблицы '{TABLE_NAME}'")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из таблицы: {e}")
        return

    # Получаем все версии ПО из VictoriaMetrics
    vm_software_versions = get_all_software_versions(time_range="3h")

    # Если нет данных из VM
    if not vm_software_versions:
        logger.warning("Не получено данных о версиях ПО из VictoriaMetrics")
        return

    # Словарь для сопоставления host+software -> row_id для быстрого поиска
    existing_entries = {}

    # Проходим по существующим строкам и создаем индекс
    for row in rows:
        host_val = row.get("Host")
        software_val = row.get("ПО")

        # Игнорируем строки с хостами, начинающимися на "pdp"
        if host_val and host_val.lower().startswith("pdp"):
            continue

        if host_val and software_val:
            normalized_host = str(host_val).lower().strip()
            normalized_software = str(software_val).strip()

            key = f"{normalized_host}::{normalized_software}"
            existing_entries[key] = {
                "row": row,
                "current_version": row.get("Версия", ""),
                "current_contour": row.get("Контур", ""),
            }

    logger.info(f"Найдено {len(existing_entries)} существующих записей в таблице")

    # Списки для новых и обновляемых записей
    rows_to_update = []
    rows_to_create = []

    # Обрабатываем данные из VictoriaMetrics
    for key, vm_data in vm_software_versions.items():
        vm_host = vm_data["host"]
        vm_software = vm_data["software"]
        vm_version = vm_data["version"]
        vm_contour = vm_data["contour"]

        # Игнорируем хосты, начинающиеся с "pdp" и из VictoriaMetrics
        if vm_host.lower().startswith("pdp"):
            continue

        if key in existing_entries:
            # Запись существует, проверяем нужно ли обновлять
            existing_data = existing_entries[key]
            current_version = existing_data["current_version"]
            current_contour = existing_data["current_contour"]

            # Проверяем изменения версии или контура
            version_changed = current_version != vm_version
            contour_changed = current_contour != vm_contour

            if version_changed or contour_changed:
                # Подготавливаем обновления
                updates = {}
                changes_log = []

                if version_changed:
                    updates["Версия"] = vm_version
                    changes_log.append(
                        f"Версия: {current_version if current_version else 'пусто'} -> {vm_version}"
                    )

                if contour_changed:
                    updates["Контур"] = vm_contour
                    changes_log.append(
                        f"Контур: {current_contour if current_contour else 'пусто'} -> {vm_contour}"
                    )

                rows_to_update.append(
                    {
                        "_id": existing_data["row"]["_id"],
                        "host": vm_host,
                        "software": vm_software,
                        "updates": updates,
                        "changes": changes_log,
                    }
                )
        else:
            # Запись не существует - нужно создать
            rows_to_create.append(
                {
                    "host": vm_host,
                    "software": vm_software,
                    "version": vm_version,
                    "contour": vm_contour,
                }
            )

    # Выводим информацию о планируемых изменениях
    if rows_to_update:
        logger.info(f"\nНайдено {len(rows_to_update)} записей для обновления:")
        for update in rows_to_update[:10]:
            logger.info(f"  Хост: {update['host']}, ПО: {update['software']}")
            for change in update["changes"]:
                logger.info(f"    {change}")
        if len(rows_to_update) > 10:
            logger.info(f"  ... и еще {len(rows_to_update) - 10} записей")

    if rows_to_create:
        logger.info(f"\nНайдено {len(rows_to_create)} новых записей для создания:")
        for new_row in rows_to_create[:10]:
            logger.info(
                f"  Хост: {new_row['host']}, Контур: {new_row['contour']}, ПО: {new_row['software']}, Версия: {new_row['version']}"
            )
        if len(rows_to_create) > 10:
            logger.info(f"  ... и еще {len(rows_to_create) - 10} записей")

    # Если нет изменений
    if not rows_to_update and not rows_to_create:
        logger.info("\nНет изменений для синхронизации.")
        return

    # Запрос подтверждения у пользователя
    # try:
    total_changes = len(rows_to_update) + len(rows_to_create)
    #     user_input = input(f"\nВыполнить синхронизацию ({total_changes} изменений) [y/N]: ").strip().lower()
    # except EOFError:
    #     user_input = 'n'
    #
    # if user_input != 'y':
    #     logger.info("Синхронизация отменена.")
    #     return

    # Выполняем обновления
    if rows_to_update:
        logger.info("\nНачинается обновление существующих записей...")
        updated_count = 0
        for update in rows_to_update:
            try:
                base.update_row(TABLE_NAME, update["_id"], update["updates"])
                updated_count += 1
                logger.debug(
                    f"Обновлен хост {update['host']}, ПО: {update['software']}"
                )
            except Exception as e:
                logger.error(
                    f"Ошибка при обновлении хоста {update['host']}, ПО {update['software']}: {e}"
                )

        logger.info(f"Обновлено {updated_count} записей.")

    # Создаем новые записи
    if rows_to_create:
        logger.info("\nНачинается создание новых записей...")
        created_count = 0
        for new_row in rows_to_create:
            try:
                base.append_row(
                    TABLE_NAME,
                    {
                        "Host": new_row["host"],
                        "Контур": new_row["contour"],
                        "ПО": new_row["software"],
                        "Версия": new_row["version"],
                    },
                )
                created_count += 1
                logger.debug(
                    f"Создан хост {new_row['host']}, Контур: {new_row['contour']}, ПО: {new_row['software']}"
                )
            except Exception as e:
                logger.error(
                    f"Ошибка при создании записи для хоста {new_row['host']}: {e}"
                )

        logger.info(f"Создано {created_count} новых записей.")

    logger.info(
        f"\nСинхронизация завершена. Всего обработано: {total_changes} записей."
    )
    logger.info(f"  - Обновлено: {len(rows_to_update)}")
    logger.info(f"  - Создано: {len(rows_to_create)}")


def main():
    """Запускает процесс синхронизации версий ПО."""
    logger.info("=" * 60)
    logger.info(
        f"Начало синхронизации версий ПО: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("=" * 60)
    sync_versions_to_seatable()


if __name__ == "__main__":
    main()
