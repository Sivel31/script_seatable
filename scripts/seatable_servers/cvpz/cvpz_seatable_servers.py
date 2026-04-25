#############################################################################################
# Скрипт получает метрики из VictoriaMetrics ЦВПЗ                                           #
# Проверяет наличие текущих данных в таблице "Серверы" SeaTable                             #
# Если данные различаются, то обновляет столбцы: Сбор метрик, CPU, RAM, информация о дисках #
#############################################################################################

import requests
import math
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
            "cvpz-update.log",
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
# 1 = test, 2 = stage, 3 = prod, 4 = prod_dc15
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

TABLE_NAME = "Серверы"

# Подключение к SeaTable
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


def round_to_even_up(value):
    """Округляет вверх до ближайшего чётного целого."""
    ceil_val = math.ceil(value)
    return ceil_val if ceil_val % 2 == 0 else ceil_val + 1


def round_up_if_last_digit_ge_6(value):
    """Округляет вверх до следующего десятка, если последняя цифра числа >= 6."""
    n = int(value)
    last_digit = n % 10
    if last_digit >= 6:
        return ((n // 10) + 1) * 10
    else:
        return n


def collect_cpu_ram_mountpoints():
    """Собирает данные по CPU, RAM, точкам монтирования и непримонтированным дискам из VictoriaMetrics."""
    cpu_data = {}
    ram_data = {}
    mountpoint_data = {}
    unmounted_data = {}

    # Обрабатываем каждый проект из списка VM_PROJECT_IDS
    for project_id in VM_PROJECT_IDS:
        # Запрос количества CPU ядер
        cpu_query = f'count without(cpu, mode) (node_cpu_seconds_total{{mode="idle", vm_project_id="{project_id}"}})'
        cpu_result = fetch_metric(cpu_query)
        for item in cpu_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            if hostname:
                try:
                    cores = int(float(item["value"][1]))
                    cpu_data[hostname] = cores
                except (ValueError, TypeError):
                    pass

        # Запрос объема оперативной памяти
        ram_query = f'sum by (mdm_hostname) (node_memory_MemTotal_bytes{{vm_project_id="{project_id}"}}) / (1024^3)'
        ram_result = fetch_metric(ram_query)
        for item in ram_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            if hostname:
                try:
                    real_gb = float(item["value"][1])
                    ram_gb = math.ceil(real_gb)
                    ram_data[hostname] = ram_gb
                except (ValueError, TypeError):
                    pass

        # Запрос информации о точках монтирования
        mp_query = f'node_filesystem_size_bytes{{fstype!="tmpfs", vm_project_id="{project_id}"}} / (1024^3)'
        mp_result = fetch_metric(mp_query)
        for item in mp_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            mountpoint = labels.get("mountpoint")
            if hostname and mountpoint:
                try:
                    size_gb = float(item["value"][1])
                    size_gb_rounded = round_to_even_up(size_gb)

                    if hostname not in mountpoint_data:
                        mountpoint_data[hostname] = {}

                    # Группировка boot разделов
                    if mountpoint == "/boot" or mountpoint.startswith("/boot/"):
                        key = "/boot"
                        mountpoint_data[hostname][key] = (
                            mountpoint_data[hostname].get(key, 0) + size_gb_rounded
                        )

                    # Группировка NFS разделов
                    elif mountpoint.startswith("/nfs/"):
                        key = "/nfs"
                        mountpoint_data[hostname][key] = (
                            mountpoint_data[hostname].get(key, 0) + size_gb_rounded
                        )

                    # Группировка data разделов
                    elif mountpoint.startswith("/data/"):
                        if mountpoint == "/data/00" or mountpoint == "/data2":
                            mountpoint_data[hostname][mountpoint] = size_gb_rounded
                        else:
                            key = "/data"
                            mountpoint_data[hostname][key] = (
                                mountpoint_data[hostname].get(key, 0) + size_gb_rounded
                            )

                    # Остальные разделы сохраняем как есть
                    else:
                        mountpoint_data[hostname][mountpoint] = size_gb_rounded

                except (ValueError, TypeError):
                    pass

        # Запрос информации о непримонтированных дисках
        unmounted_query = f'last_over_time(scsiId_disk_info{{device!="/dev/sr0", mountpoint="n/a", vm_project_id="{project_id}"}}[1h])'
        unmounted_result = fetch_metric(unmounted_query)
        for item in unmounted_result:
            labels = item["metric"]
            hostname = labels.get("mdm_hostname")
            total_size = labels.get("total_size_disk")
            if hostname and total_size:
                try:
                    size_bytes = float(total_size)
                    size_gb = size_bytes / (1024**3)
                    size_gb_rounded = round_to_even_up(size_gb)
                    unmounted_data[hostname] = (
                        unmounted_data.get(hostname, 0) + size_gb_rounded
                    )
                except (ValueError, TypeError):
                    pass

    # Расчет общего объема HDD (сумма всех примонтированных томов, кроме /nfs)
    hdd_total_data = {}
    EXCLUDE_MOUNTPOINTS = {"/nfs"}

    for host, mounts in mountpoint_data.items():
        total = sum(
            size for mp, size in mounts.items() if mp not in EXCLUDE_MOUNTPOINTS
        )
        if total > 0:
            hdd_total_data[host] = round_up_if_last_digit_ge_6(total)

    return cpu_data, ram_data, mountpoint_data, hdd_total_data, unmounted_data


def sync_cpu_ram_mountpoints_to_seatable():
    """Синхронизирует метрики с таблицей, корректирует статус сбора метрик"""
    # Получаем все строки из таблицы SeaTable
    rows = base.list_rows(TABLE_NAME)
    if not rows:
        logger.info("Таблица пуста.")
        return

    # Собираем метрики из VictoriaMetrics
    cpu_data, ram_data, mountpoint_data, hdd_total_data, unmounted_data = (
        collect_cpu_ram_mountpoints()
    )

    # Формируем множество всех хостов, для которых есть метрики
    all_vm_hosts = (
        set(cpu_data.keys())
        | set(ram_data.keys())
        | set(mountpoint_data.keys())
        | set(hdd_total_data.keys())
        | set(unmounted_data.keys())
    )
    logger.info(f"Найдено данных для {len(all_vm_hosts)} хостов.")

    # Фильтруем строки из SeaTable: оставляем только с заполненным полем Host и исключаем хосты с pdp
    host_rows = []
    for row in rows:
        host_val = row.get("Host")
        if host_val is not None and str(host_val).strip():
            host_key = str(host_val).strip()
            if host_key.startswith("pdp"):
                continue
            host_rows.append((host_key, row))

    # Список для хранения обновлений
    updates = []

    # Проверяем каждую строку на необходимость обновления
    for host_key, row in host_rows:
        current_flag = row.get("Сбор метрик")
        has_metrics = host_key in all_vm_hosts

        # Определяем ожидаемое значение флага "Сбор метрик"
        expected_flag = "Да" if has_metrics else "Нет"

        row_updates = {}
        changes_log = []
        update_needed = False

        # Обновляем флаг "Сбор метрик" если он изменился
        if current_flag != expected_flag:
            row_updates["Сбор метрик"] = expected_flag
            old_display = (
                current_flag
                if current_flag is not None and current_flag != ""
                else "пусто"
            )
            changes_log.append(f"Сбор метрик: {old_display} -> {expected_flag}")
            update_needed = True

        # Если для хоста есть метрики, обновляем остальные поля
        if has_metrics:
            vm_hostname = host_key

            current_cpu = row.get("CPU")
            current_ram = row.get("RAM")
            current_hdd = row.get("HDD Total")

            cpu_new = cpu_data.get(vm_hostname)
            ram_new = ram_data.get(vm_hostname)
            mp_dict = mountpoint_data.get(vm_hostname, {})
            hdd_new = hdd_total_data.get(vm_hostname)
            unmounted_size = unmounted_data.get(vm_hostname)

            # Обновление CPU
            if cpu_new is not None:
                try:
                    current_cpu_int = (
                        int(current_cpu) if current_cpu not in (None, "") else None
                    )
                except (ValueError, TypeError):
                    current_cpu_int = None
                if current_cpu_int is None or current_cpu_int != cpu_new:
                    row_updates["CPU"] = cpu_new
                    old_val = (
                        current_cpu
                        if current_cpu is not None and current_cpu != ""
                        else "пусто"
                    )
                    changes_log.append(f"CPU: {old_val} -> {cpu_new}")
                    update_needed = True

            # Обновление RAM
            if ram_new is not None:
                try:
                    current_ram_int = (
                        int(current_ram) if current_ram not in (None, "") else None
                    )
                except (ValueError, TypeError):
                    current_ram_int = None
                if current_ram_int is None or current_ram_int != ram_new:
                    row_updates["RAM"] = ram_new
                    old_val = (
                        current_ram
                        if current_ram is not None and current_ram != ""
                        else "пусто"
                    )
                    changes_log.append(f"RAM: {old_val} -> {ram_new}")
                    update_needed = True

            # Обновление HDD Total
            if hdd_new is not None:
                try:
                    current_hdd_int = (
                        int(current_hdd) if current_hdd not in (None, "") else None
                    )
                except (ValueError, TypeError):
                    current_hdd_int = None
                if current_hdd_int is None or current_hdd_int != hdd_new:
                    row_updates["HDD Total"] = hdd_new
                    old_val = (
                        current_hdd
                        if current_hdd is not None and current_hdd != ""
                        else "пусто"
                    )
                    changes_log.append(f"HDD Total: {old_val} -> {hdd_new}")
                    update_needed = True

            # Обновление точек монтирования
            for mountpoint in mp_dict:
                col_name = f"mnt {mountpoint}"
                size_gb = mp_dict[mountpoint]
                current_val = row.get(col_name)
                try:
                    current_int = (
                        int(current_val) if current_val not in (None, "") else None
                    )
                except (ValueError, TypeError):
                    current_int = None
                if current_int is None or current_int != size_gb:
                    row_updates[col_name] = size_gb
                    old_val = (
                        current_val
                        if current_val is not None and current_val != ""
                        else "пусто"
                    )
                    changes_log.append(f"{col_name}: {old_val} -> {size_gb}")
                    update_needed = True

            # Обновление непримонтированных дисков (mnt na)
            col_name_na = "mnt na"
            current_val_na = row.get(col_name_na)
            try:
                current_int_na = (
                    int(current_val_na) if current_val_na not in (None, "") else None
                )
            except (ValueError, TypeError):
                current_int_na = None

            if unmounted_size is not None:
                if current_int_na is None or current_int_na != unmounted_size:
                    row_updates[col_name_na] = unmounted_size
                    old_val = (
                        current_val_na if current_val_na not in (None, "") else "пусто"
                    )
                    changes_log.append(f"{col_name_na}: {old_val} -> {unmounted_size}")
                    update_needed = True

        # Если есть изменения, добавляем в список обновлений
        if update_needed:
            updates.append(
                {
                    "_id": row["_id"],
                    "host": host_key,
                    "updates": row_updates,
                    "changes": changes_log,
                }
            )

    # Обработка найденных обновлений
    if updates:
        logger.info(f"\nПланируется обновление {len(updates)} строк:")
        for upd in updates:
            logger.info(f"  Хост: {upd['host']} | {'; '.join(upd['changes'])}")

        # Запрос подтверждения
        # try:
        #     user_input = input("\nОбновить данные [y/N]: ").strip().lower()
        # except EOFError:
        #     user_input = 'n'
        #
        user_input = "y"
        # Выполнение обновлений при подтверждении
        if user_input == "y":
            logger.info("Начинается обновление...")
            for upd in updates:
                try:
                    base.update_row(TABLE_NAME, upd["_id"], upd["updates"])
                except Exception as e:
                    logger.error(f"Ошибка при обновлении хоста {upd['host']}: {e}")
            logger.info(f"\nВсего обновлено: {len(updates)} строк.")
        else:
            logger.info("Обновление отменено.")
    else:
        logger.info("Нет изменений для обновления.")


def main():
    """Запускает процесс синхронизации."""
    logger.info("=" * 60)
    logger.info(f"Начало синхронизации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    sync_cpu_ram_mountpoints_to_seatable()


if __name__ == "__main__":
    main()
