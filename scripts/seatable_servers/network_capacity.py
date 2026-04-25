#############################################################################################
# Скрипт рассчитывает и обновляет ёмкость сети и количество свободных адресов               #
# на основе данных из таблицы "Серверы" SeaTable                                            #
# Учитывает кластерные IP из столбца "IP cluster"                                           #
#############################################################################################

import os
import ipaddress
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
            "network-capacity-update.log",
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


def calculate_network_capacity(network_cidr):
    """
    Рассчитывает общую ёмкость сети по CIDR.

    Args:
        network_cidr (str): Сеть в CIDR, например "10.15.126.0/25"

    Returns:
        int: Общее количество IP адресов в подсети
    """
    try:
        if not network_cidr or network_cidr.strip() == "":
            return None

        # Создаем объект сети из CIDR
        network = ipaddress.IPv4Network(network_cidr.strip(), strict=False)
        capacity = network.num_addresses
        return capacity
    except Exception as e:
        logger.error(f"Ошибка расчета ёмкости для сети {network_cidr}: {e}")
        return None


def update_network_capacity_and_free_ips():
    """
    Обновляет столбцы 'Ёмкость сети' и 'Количество свободных адресов'
    для всех строк таблицы на основе подсетей.
    Учитывает кластерные IP из столбца "IP cluster".
    """
    logger.info("=" * 60)
    logger.info(
        f"Начало расчета ёмкости сетей: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("=" * 60)

    # Получаем все строки из таблицы
    rows = base.list_rows(TABLE_NAME)
    if not rows:
        logger.info("Таблица пуста.")
        return

    logger.info(f"Получено {len(rows)} строк из таблицы.")

    # Группируем хосты по подсетям (Network - CIDR)
    subnets = {}
    for row in rows:
        network = row.get("Network")
        if network and str(network).strip():
            network_key = str(network).strip()
            if network_key not in subnets:
                subnets[network_key] = []
            subnets[network_key].append(row)

    logger.info(f"Найдено {len(subnets)} подсетей.")

    # Список для хранения обновлений
    updates = []

    # Обрабатываем каждую подсеть
    for network_cidr, subnet_rows in subnets.items():
        # Рассчитываем ёмкость сети из CIDR
        capacity = calculate_network_capacity(network_cidr)

        if capacity is None:
            logger.warning(f"Не удалось рассчитать ёмкость для подсети {network_cidr}")
            continue

        # Считаем количество VM в подсети
        vm_count = len(subnet_rows)

        # Считаем кластерные IP (если поле "IP cluster" заполнено)
        cluster_ips_count = sum(
            1
            for row in subnet_rows
            if row.get("IP cluster") and str(row.get("IP cluster")).strip()
        )

        # Рассчитываем свободные адреса
        # Формула: ёмкость - 4 (зарезервированные) - VM - кластерные IP
        reserved_ips = 4
        free_ips = capacity - reserved_ips - vm_count - cluster_ips_count

        # Убеждаемся, что free_ips не отрицательный
        free_ips = max(0, free_ips)

        logger.info(
            f"Подсеть {network_cidr}: ёмкость={capacity}, VM={vm_count}, "
            f"cluster_IP={cluster_ips_count}, свободно={free_ips}"
        )

        # Добавляем обновления для всех строк в подсети
        for row in subnet_rows:
            row_updates = {
                "Ёмкость сети": capacity,
                "Количество свободных адресов": free_ips,
            }

            updates.append(
                {
                    "_id": row["_id"],
                    "host": row.get("Host", "Unknown"),
                    "network": network_cidr,
                    "updates": row_updates,
                }
            )

    # Применяем обновления
    if updates:
        logger.info(f"\nПланируется обновление {len(updates)} строк.")

        # Запрос подтверждения
        # try:
        #     user_input = input("\nОбновить данные [y/N]: ").strip().lower()
        # except EOFError:
        #     user_input = 'n'
        user_input = "y"
        if user_input == "y":
            logger.info("Начинается обновление...")
            success_count = 0
            error_count = 0

            for upd in updates:
                try:
                    base.update_row(TABLE_NAME, upd["_id"], upd["updates"])
                    success_count += 1
                except Exception as e:
                    logger.error(f"Ошибка при обновлении хоста {upd['host']}: {e}")
                    error_count += 1

            logger.info(f"\nВсего обновлено: {success_count} строк.")
            if error_count > 0:
                logger.warning(f"Ошибок: {error_count}")
        else:
            logger.info("Обновление отменено.")
    else:
        logger.info("Нет данных для обновления.")

    logger.info("=" * 60)
    logger.info(f"Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


def main():
    """Запускает процесс расчета и обновления."""
    update_network_capacity_and_free_ips()


if __name__ == "__main__":
    main()

