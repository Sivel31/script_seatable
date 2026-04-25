#####################################################################################
# Скрипт получает список прикладного ПО (ППО) из VictoriaMetrics ЦВПЗ и их версий   #
# Загружает в таблицу "Версии ППО"                                                  #
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
            "ppo_versions_update.log",
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

TABLE_NAME = "Версии ППО"

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
    """Получает все версии прикладного ПО со всех хостов."""
    software_versions = {}

    # Конфигурация запросов
    queries = [
        {
            "name": "kri",
            "query": 'ispctl_status_info{job="script_exporter_ispctl_exporter", mdm_subsys="kri", vm_project_id="%s"}',
            "software_field": "module",
            "version_field": "version",
            "subsystem": "kri",
        },
        {
            "name": "kri_links",
            "query": 'ispctl_status_info{job="script_exporter_ispctl_exporter", mdm_subsys="kri_links", vm_project_id="%s"}',
            "software_field": "module",
            "version_field": "version",
            "subsystem": "kri_links",
        },
        {
            "name": "pdp",
            "query": 'ispctl_status_info{job="script_exporter_ispctl_exporter", mdm_subsys="pdp", vm_project_id="%s"}',
            "software_field": "module",
            "version_field": "version",
            "subsystem": "pdp",
        },
        {
            "name": "tech_portal",
            "query": 'script_tech_portal_version{job="script_exporter_tech_portal", mdm_subsys="tech_portal", vm_project_id="%s"}',
            "software_field": "service",
            "version_field": "version",
            "subsystem": "tech_portal",
        },
        {
            "name": "ksrd",
            "query": 'script_ksrd_version{job=~"script_exporter_ksrd|script_exporter_ksrd4", mdm_subsys="ksrd", vm_project_id="%s"}',
            "software_field": "mdm_service",
            "version_field": "unidataVersion",
            "subsystem": "ksrd",
        },
        {
            "name": "kzd",
            "query": 'script_kzd_version{job="script_exporter_kzd", mdm_subsys="kzd", vm_project_id="%s"}',
            "software_field": "service",
            "version_field": "version",
            "subsystem": "kzd",
        },
    ]

    logger.info("Сбор данных о версиях ПО...")

    for project_id in VM_PROJECT_IDS:
        for config in queries:
            # Формируем запрос с подстановкой project_id
            query = config["query"] % project_id
            full_query = f"last_over_time(({query})[{time_range}])"

            logger.debug(f"Запрос {config['name']} для project_id={project_id}")
            result = fetch_metric(full_query)

            for item in result:
                labels = item["metric"]
                hostname = labels.get("mdm_hostname")
                software = labels.get(config["software_field"])
                version = labels.get(config["version_field"])
                mdm_env = labels.get("mdm_env", "")

                if hostname and software and version:
                    normalized_hostname = hostname.lower().strip()
                    key = f"{normalized_hostname}::{config['subsystem']}::{software}"

                    # Проверка на дубли
                    if key in software_versions:
                        # Особые правила для некоторых подсистем
                        if config["subsystem"] in ["pdp", "kzd"]:
                            if version.lower() == "unknown":
                                existing = software_versions[key]
                                if existing["version"].lower() != "unknown":
                                    # Пропускаем unknown если уже есть конкретная версия
                                    continue
                        # Для остальных - пропускаем дубли
                        continue

                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан",
                        "subsystem": config["subsystem"],
                    }

    logger.info(f"Получено {len(software_versions)} уникальных записей")
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
        subsystem_val = row.get("Подсистема")

        if host_val and software_val and subsystem_val:
            normalized_host = str(host_val).lower().strip()
            normalized_software = str(software_val).strip()
            normalized_subsystem = str(subsystem_val).strip()

            key = f"{normalized_host}::{normalized_subsystem}::{normalized_software}"
            existing_entries[key] = {
                "row": row,
                "current_version": row.get("Версия", ""),
                "current_contour": row.get("Контур", ""),
                "current_subsystem": normalized_subsystem,
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
        vm_subsystem = vm_data["subsystem"]

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
                        "subsystem": vm_subsystem,
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
                    "subsystem": vm_subsystem,
                }
            )

    # Выводим информацию о планируемых изменениях
    if rows_to_update:
        logger.info(f"\nНайдено {len(rows_to_update)} записей для обновления:")
        for update in rows_to_update[:10]:
            logger.info(
                f"  Хост: {update['host']}, Подсистема: {update['subsystem']}, ПО: {update['software']}"
            )
            for change in update["changes"]:
                logger.info(f"    {change}")
        if len(rows_to_update) > 10:
            logger.info(f"  ... и еще {len(rows_to_update) - 10} записей")

    if rows_to_create:
        logger.info(f"\nНайдено {len(rows_to_create)} новых записей для создания:")
        for new_row in rows_to_create[:10]:
            logger.info(
                f"  Хост: {new_row['host']}, Контур: {new_row['contour']}, "
                f"Подсистема: {new_row['subsystem']}, ПО: {new_row['software']}, "
                f"Версия: {new_row['version']}"
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
                    f"Обновлен хост {update['host']}, Подсистема: {update['subsystem']}, ПО: {update['software']}"
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
                        "Подсистема": new_row["subsystem"],
                        "ПО": new_row["software"],
                        "Версия": new_row["version"],
                    },
                )
                created_count += 1
                logger.debug(
                    f"Создан хост {new_row['host']}, Контур: {new_row['contour']}, "
                    f"Подсистема: {new_row['subsystem']}, ПО: {new_row['software']}"
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
        f"Начало синхронизации версий прикладного ПО: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("=" * 60)
    sync_versions_to_seatable()


if __name__ == "__main__":
    main()
