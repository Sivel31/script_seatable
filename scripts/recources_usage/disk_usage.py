##################################################################################################
# Скрипт получает метрики из VictoriaMetrics ЦВПЗ                                                #
# Считает заполненность дискового пространства и загружает её в таблицу Нехватка ресурсов (Диск) #
##################################################################################################

import requests
import math
import os
import urllib3
from seatable_api import Base
import logging

# Настройка модуля logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

# Отключаем SSL-предупреждения
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# VictoriaMetrics Config
VMSELECT_URL = "https://cvpz-user-vmauth.data.corp/api/v1/query"
TOKEN = os.getenv("VICTORIA_TOKEN")
if not TOKEN:
    logger.error("Environment variable VICTORIA_TOKEN is not set")
    raise ValueError("Environment variable VICTORIA_TOKEN is not set")

VM_ACCOUNT_ID = "2"
VM_PROJECT_IDS = ["1", "2", "3", "4"]  # test, stage, prod_dc05, prod_dc15
TIMEOUT = 10

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "TenantID": VM_ACCOUNT_ID
}

# SeaTable
SEATABLE_TOKEN = os.getenv("API_SEATABLE_MDM")
if not SEATABLE_TOKEN:
    logger.error("Environment variable API_SEATABLE_MDM is not set")
    raise ValueError("Environment variable API_SEATABLE_MDM is not set")

SERVER_URL = os.getenv("SEATABLE_SERVER")
if not SERVER_URL:
    logger.error("Environment variable SEATABLE_SERVER is not set")
    raise ValueError("Environment variable SEATABLE_SERVER is not set")

TABLE_NAME = "Нехватка ресурсов (Диск)"

# Auth SeaTable
base = Base(SEATABLE_TOKEN, SERVER_URL)
base.auth()

def fetch_metric(query):
    """Выполняет PromQL запрос к VictoriaMetrics"""
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
        if data["status"] == "success":
            return data["data"]["result"]
        else:
            logger.error(f"Ошибка в запросе {query}: {data.get('error', 'Unknown error')}")
            return []

    except requests.RequestException as error:
        logger.error(f"Ошибка выполнения {query}: {error}")
        return []

def get_hostname(labels, instance):
    """Возвращает mdm_hostname если есть, иначе instance."""
    return labels.get("mdm_hostname") or instance

def get_env(labels):
    """Возвращает mdm_env если есть, иначе пустую строку."""
    return labels.get("mdm_env", "")

def collect_disk_resources(threshold_percent=80):
    """Собирает данные по диску со всех проектов и фильтрует по общему использованию на сервере."""
    servers = {}
    server_totals = {}

    for project_id in VM_PROJECT_IDS:
        QUERIES = {
            "total_disk": f'sum by (instance, mdm_hostname, device, mdm_env) (node_filesystem_size_bytes{{device!~"tmpfs|overlay", vm_project_id="{project_id}"}})',
            "avail_disk": f'sum by (instance, mdm_hostname, device, mdm_env) (node_filesystem_avail_bytes{{device!~"tmpfs|overlay", vm_project_id="{project_id}"}})'
        }

        total_data = fetch_metric(QUERIES["total_disk"])
        avail_data = fetch_metric(QUERIES["avail_disk"])

        # Сбор данных по total
        for item in total_data:
            labels = item["metric"]
            instance = labels["instance"]
            hostname = get_hostname(labels, instance)
            device = labels.get("device", "/")
            env = get_env(labels)
            total_gb = math.ceil(float(item["value"][1]) / (1024 ** 3))

            if instance not in servers:
                servers[instance] = []
                server_totals[instance] = {"hostname": hostname, "env": env, "total_gb": 0, "used_gb": 0}

            servers[instance].append({
                "device": device,
                "total_gb": total_gb,
                "used_gb": None,
                "hostname": hostname,
                "env": env
            })

            server_totals[instance]["total_gb"] += total_gb

        # Сбор данных по avail и вычисление used
        for item in avail_data:
            labels = item["metric"]
            instance = labels["instance"]
            device = labels.get("device", "/")
            avail_gb = math.ceil(float(item["value"][1]) / (1024 ** 3))

            if instance in servers:
                for point in servers[instance]:
                    if point["device"] == device:
                        point["used_gb"] = point["total_gb"] - avail_gb
                        server_totals[instance]["used_gb"] += point["used_gb"]
                        break

    # Формирование финальной таблицы
    table = []

    for instance, points in servers.items():
        total_gb = server_totals[instance]["total_gb"]
        used_gb = server_totals[instance]["used_gb"]
        hostname = server_totals[instance]["hostname"]
        env = server_totals[instance]["env"]

        if total_gb > 0:
            overall_percent = (used_gb / total_gb) * 100
        else:
            overall_percent = 0

        # Если общий процент >= порога — добавляем точки монтирования
        if overall_percent >= threshold_percent:
            for point in points:
                if point["used_gb"] is not None:
                    device = point["device"]
                    # Фильтр дисков
                    if (device.startswith("/dev/sda") or
                        device == "rootfs" or
                        device == "/dev/mapper/rhel-root" or
                        device == "/dev/mapper/rhel-root" or
                        device == "/dev/mapper/system-root" or
                        device == "/dev/mapper/vg-root" or
                        device == "/dev/mapper/vg-var" or
                        device == "/dev/mapper/vg_sys-lv_root" or
                        device == "/dev/mapper/system-roott"):
                        continue

                    used_percent = (point["used_gb"] / point["total_gb"]) * 100 if point["total_gb"] > 0 else 0

                    table.append({
                        "Сервер": hostname,
                        "Контур": point["env"],
                        "Диск": point["device"],
                        "Сколько занято места (ГБ)": point["used_gb"],
                        "Сколько всего места (ГБ)": point["total_gb"],
                        "Процент использования": int(used_percent)
                    })

    return table

def sync_with_seatable(table_name, new_rows):
    """Удаляются текущие строки, загружаются новые."""
    if not new_rows:
        logger.info("Нет данных для загрузки в SeaTable.")
        return

    try:
        existing_rows = base.list_rows(table_name)
        row_ids = [row["_id"] for row in existing_rows if "_id" in row]

        if row_ids:
            logger.info(f"Удаление {len(row_ids)} существующих строк...")
            base.batch_delete_rows(table_name, row_ids)
            logger.info("Все старые строки удалены.")

        logger.info(f"Добавление {len(new_rows)} новых строк...")
        for row in new_rows:
            base.append_row(table_name, row)
            logger.info(f"Добавлен: {row['Сервер']} — {row['Контур']} — {row['Диск']} — {row['Процент использования']}%")

        logger.info(f"\nЗагружено {len(new_rows)} строк.")

    except Exception as e:
        logger.error(f"Ошибка при синхронизации с SeaTable: {e}")

def main():
    logger.info("Сбор данных по заполненности дисков...")
    # threshold_percent задан 80%, он считается по общей заполненности диска
    rows = collect_disk_resources(threshold_percent=80)

    logger.info(f"Найдено дисков с высокой заполненностью: {len(rows)}")

    # Загрузка в SeaTable
    logger.info("Загрузка в SeaTable...")
    sync_with_seatable(TABLE_NAME, rows)

if __name__ == "__main__":
    main()