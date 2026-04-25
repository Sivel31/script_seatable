#####################################################################################
# Скрипт получает список СПО и их версий, формирует csv-файл                        #
#####################################################################################

import requests
import os
import urllib3
from datetime import datetime
import csv
import logging


# Настройка модуля logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
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
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "TenantID": VM_ACCOUNT_ID
}

# Имя CSV файла для экспорта
CSV_FILENAME = "software_versions.csv"

def fetch_metric(query):
    """Выполняет запрос к VictoriaMetrics и возвращает результат метрики."""
    try:
        response = requests.get(
            VMSELECT_URL,
            params={"query": query},
            headers=HEADERS,
            timeout=TIMEOUT,
            verify=False
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
                    "contour": mdm_env if mdm_env else "не указан"
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
                        "contour": mdm_env if mdm_env else "не указан"
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
                        "contour": mdm_env if mdm_env else "не указан"
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
                        "contour": mdm_env if mdm_env else "не указан"
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
                    version_parts = version.split('.')
                    major_version = int(version_parts[0]) if version_parts else 0

                    # Если версия меньше 5 - это Opensearch (ES 1.x, 2.x), иначе Elasticsearch
                    if major_version < 5:
                        software_name = "Opensearch"
                    else:
                        software_name = "Elasticsearch"

                    logger.debug(f"ES версия {version} -> {software_name} (мажорная: {major_version})")

                except (ValueError, AttributeError, IndexError):
                    # Если не удалось распарсить версию, используем по умолчанию
                    software_name = "Elasticsearch"
                    logger.debug(f"Не удалось распарсить версию ES: {version}, используем по умолчанию {software_name}")

                key = f"{normalized_hostname}::{software_name}"

                # Добавляем только если такой записи еще нет
                if key not in software_versions:
                    software_versions[key] = {
                        "host": normalized_hostname,
                        "software": software_name,
                        "version": version,
                        "contour": mdm_env if mdm_env else "не указан"
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
            version = labels.get("server_version")

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
                        "contour": mdm_env if mdm_env else "не указан"
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
            version = labels.get("short_version")

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
                        "contour": mdm_env if mdm_env else "не указан"
                    }

    # Статистика
    software_stats = {}
    for data in software_versions.values():
        software = data["software"]
        software_stats[software] = software_stats.get(software, 0) + 1

    logger.info(f"Получено данных о версиях ПО для {len(set(v['host'] for v in software_versions.values()))} хостов")
    logger.info(f"Всего уникальных записей ПО: {len(software_versions)}")

    for software, count in sorted(software_stats.items()):
        logger.info(f"  {software}: {count} записей")

    return software_versions


def save_to_csv(software_versions):
    """Сохраняет данные о версиях ПО в CSV файл."""
    try:
        # Преобразуем данные в список строк для CSV
        rows = []
        for vm_data in software_versions.values():
            vm_host = vm_data["host"]

            rows.append({
                "Host": vm_data["host"],
                "Контур": vm_data["contour"],
                "ПО": vm_data["software"],
                "Версия": vm_data["version"]
            })

        # Сортируем строки по хосту и ПО
        rows.sort(key=lambda x: (x["Host"], x["ПО"]))

        # Записываем в CSV
        with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["Host", "Контур", "ПО", "Версия"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        logger.info(f"Данные успешно сохранены в {CSV_FILENAME}")
        logger.info(f"Всего сохранено записей: {len(rows)}")

        # Выводим пример первых записей
        if rows:
            logger.info("\nПример сохраненных данных (первые 10 записей):")
            for i, row in enumerate(rows[:10]):
                logger.info(f"  {i+1}. Хост: {row['Host']}, Контур: {row['Контур']}, ПО: {row['ПО']}, Версия: {row['Версия']}")
            if len(rows) > 10:
                logger.info(f"  ... и еще {len(rows) - 10} записей")

    except Exception as e:
        logger.error(f"Ошибка при записи в CSV файл: {e}")


def main():
    """Запускает процесс экспорта версий ПО в CSV."""
    logger.info("="*60)
    logger.info(f"Начало экспорта версий ПО: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)

    # Получаем все версии ПО из VictoriaMetrics
    vm_software_versions = get_all_software_versions(time_range="3h")

    # Если нет данных из VM
    if not vm_software_versions:
        logger.warning("Не получено данных о версиях ПО из VictoriaMetrics")
        return

    # Сохраняем в CSV
    save_to_csv(vm_software_versions)

    logger.info(f"\nЭкспорт завершен. Файл сохранен: {CSV_FILENAME}")


if __name__ == "__main__":
    main()