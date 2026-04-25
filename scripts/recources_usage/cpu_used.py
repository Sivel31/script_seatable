#############################################################################################
# Скрипт получает метрики из VictoriaMetrics ЦВПЗ                                           #
# Считает среднюю нагрузку по LA и загружает её в таблицу Нехватка ресурсов (CPU)           #
#############################################################################################

import os
import requests
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

# Отключаем SSL-предупреждения (самоподписный сертификат выдаёт ошибки)
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

# SeaTable Config
SEATABLE_TOKEN = os.getenv("API_SEATABLE_MDM")
if not SEATABLE_TOKEN:
    logger.error("Environment variable API_SEATABLE_MDM is not set")
    raise ValueError("Environment variable API_SEATABLE_MDM is not set")

SERVER_URL = os.getenv("SEATABLE_SERVER")
if not SERVER_URL:
    logger.error("Environment variable SEATABLE_SERVER is not set")
    raise ValueError("Environment variable SEATABLE_SERVER is not set")

TABLE_NAME = "Нехватка ресурсов (CPU)"

# Seatable auth
base = Base(SEATABLE_TOKEN, SERVER_URL)
base.auth()


def fetch_metric(query):
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
            logger.error(f"Ошибка в запросе: {data.get('error', 'Unknown error')}")
            return []

    except requests.RequestException as error:
        logger.error(f"Ошибка выполнения запроса: {error}")
        return []


def get_hostname(labels):
    """Возвращает mdm_hostname если есть, иначе значение unkown."""
    return labels.get("mdm_hostname", "unknown")

def get_env(labels):
    """Возвращает mdm_env если есть, иначе пустую строку."""
    return labels.get("mdm_env", "")


def collect_cpu_overload_by_la():
    """Собирает данные по CPU со всех проектов."""
    all_servers = {}

    for project_id in VM_PROJECT_IDS:
        la_query = f'''
        avg_over_time(node_load5{{vm_project_id="{project_id}"}}[7d:])
        '''

        cores_query = f'''
        count without(cpu, mode) (node_cpu_seconds_total{{mode="idle", vm_project_id="{project_id}"}})
        '''

        la_data = fetch_metric(la_query)
        cores_data = fetch_metric(cores_query)

        for item in cores_data:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            if not hostname:
                continue
            if hostname not in all_servers:
                all_servers[hostname] = {
                    "Количество ядер": int(float(item["value"][1])),
                    "Средний LA5 за 7 дней": 0.0,
                    "Контур": get_env(labels)
                }

        for item in la_data:
            labels = item["metric"]
            hostname = get_hostname(labels)
            env = get_env(labels)
            avg_la = float(item["value"][1])

            if hostname not in all_servers:
                all_servers[hostname] = {
                    "Количество ядер": 1,
                    "Средний LA5 за 7 дней": 0.0,
                    "Контур": env
                }

            all_servers[hostname]["Средний LA5 за 7 дней"] = avg_la

    result = []
    for hostname, data in all_servers.items():
        cores = data["Количество ядер"]
        avg_la = data["Средний LA5 за 7 дней"]
        la_per_core = avg_la / cores if cores > 0 else 0

        # Можно скорректировать пороговое значение
        if la_per_core >= 0.9:
            result.append({
                "Сервер": hostname,
                "Контур": data["Контур"],
                "Количество ядер": cores,
                "Средний LA5 за 7 дней": round(avg_la, 2),
                "LA5 / Ядра": round(la_per_core, 2)
            })

    return result


def sync_with_seatable(table_name, new_rows):
    """Удаляются текущие строки, загружаются новые."""
    if not new_rows:
        logger.info("Нет серверов с высокой нагрузкой по CPU.")
        return

    try:
        existing_rows = base.list_rows(table_name)
        row_ids = [row["_id"] for row in existing_rows if "_id" in row]

        if row_ids:
            logger.info(f"Удаление {len(row_ids)} существующих строк...")
            base.batch_delete_rows(table_name, row_ids)

        logger.info(f"Добавление {len(new_rows)} серверов...")
        for row in new_rows:
            base.append_row(table_name, row)
            logger.info(f"Добавлен: {row['Сервер']} — {row['Количество ядер']} ядер, LA5: {row['Средний LA5 за 7 дней']}")

        logger.info(f"Успешно загружено {len(new_rows)} серверов.")

    except Exception as e:
        logger.error(f"Ошибка при синхронизации с SeaTable: {e}")


def main():
    logger.info("Сбор данных по среднему LA5 за 7 дней...")
    rows = collect_cpu_overload_by_la()

    logger.info(f"Найдено серверов с высокой нагрузкой: {len(rows)}")
    for row in rows:
        logger.info(f"  → {row['Сервер']} ({row['Контур']}) — {row['Количество ядер']} ядер, LA5: {row['Средний LA5 за 7 дней']}, LA5/Ядра: {row['LA5 / Ядра']}")

    logger.info("Загрузка в SeaTable...")
    sync_with_seatable(TABLE_NAME, rows)


if __name__ == "__main__":
    main()
