############################################################################################
# Из таблицы "Версии СПО" получает список ПО и их версий, на основе значения столбца Host  #
# заполняет столбцы "Софт (факт)" и "Версия реальная"                                      #
############################################################################################

import os
import logging
from datetime import datetime
from seatable_api import Base

# Настройка модуля logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

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

# Создаем подключение к SeaTable
base = Base(SEATABLE_TOKEN, SERVER_URL)
base.auth()

# Имена таблиц
VERSIONS_TABLE = "Версии СПО"
SERVERS_TABLE = "Серверы"

# True - множественный выбор, False - текстовый столбец
SOFTWARE_COLUMN_TYPE = True  # столбец "Софт факт"
VERSIONS_COLUMN_TYPE = False  # столбец "Версия реальная"


def get_software_by_host():
    """Получает из таблицы 'Версии СПО' список ПО и версий для каждого хоста."""
    try:
        rows = base.list_rows(VERSIONS_TABLE)
        logger.info(f"Загружено {len(rows)} строк из таблицы '{VERSIONS_TABLE}'")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из таблицы '{VERSIONS_TABLE}': {e}")
        return {}, {}

    # Словари: host -> данные
    host_software_names = {}  # список названий ПО
    host_software_versions = {}  # словарь ПО->версия

    for row in rows:
        host_val = row.get("Host")
        software_val = row.get("ПО")
        version_val = row.get("Версия")

        if host_val and software_val:
            # Нормализуем имя хоста для сравнения
            normalized_host = str(host_val).lower().strip()
            software = str(software_val).strip()
            version = str(version_val).strip() if version_val else "не указана"

            # Инициализируем структуры для хоста если нужно
            if normalized_host not in host_software_names:
                host_software_names[normalized_host] = []
                host_software_versions[normalized_host] = {}

            # Добавляем ПО в список названий
            if software not in host_software_names[normalized_host]:
                host_software_names[normalized_host].append(software)

            # Добавляем версию
            host_software_versions[normalized_host][software] = version

    # Сортируем ПО для каждого хоста
    for host in host_software_names:
        host_software_names[host].sort()
        # Сортируем и словарь версий
        host_software_versions[host] = dict(
            sorted(host_software_versions[host].items())
        )

    logger.info(f"Найдено {len(host_software_names)} хостов с информацией о ПО")

    return host_software_names, host_software_versions


def prepare_column_data(value, column_type_multiselect):
    """Подготавливает данные для столбца в зависимости от его типа."""
    if column_type_multiselect:
        # Для множественного выбора - возвращаем список
        return value
    else:
        # Для текстового столбца - возвращаем строку
        if isinstance(value, list):
            return " ".join(value)
        elif isinstance(value, dict):
            pairs = [f"{k}={v}" for k, v in value.items()]
            return " ".join(pairs)
        else:
            return str(value)


def compare_values(current, new, is_multiselect):
    """Сравнивает значения с учетом типа столбца."""
    if is_multiselect:
        # Для множественного выбора сравниваем отсортированные массивы
        if isinstance(current, list) and isinstance(new, list):
            return sorted(current) != sorted(new)
        else:
            # Если один из аргументов не список, считаем разные
            return True
    else:
        # Для текстового столбца сравниваем строки
        current_str = str(current) if current is not None else ""
        new_str = str(new) if new is not None else ""
        return current_str != new_str


def sync_software_to_servers(host_software_names, host_software_versions):
    """Синхронизирует информацию о ПО и версиях в таблицу 'Серверы-test'."""
    try:
        servers = base.list_rows(SERVERS_TABLE)
        logger.info(f"Загружено {len(servers)} строк из таблицы '{SERVERS_TABLE}'")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из таблицы '{SERVERS_TABLE}': {e}")
        return

    # Создаем индекс серверов для быстрого поиска
    server_index = {}
    for server in servers:
        host_val = server.get("Host")
        if host_val:
            normalized_host = str(host_val).lower().strip()
            server_index[normalized_host] = {
                "row": server,
                "current_software": server.get("Софт факт", ""),
                "current_versions": server.get("Версия реальная", ""),
            }

    logger.info(f"Найдено {len(server_index)} серверов в таблице '{SERVERS_TABLE}'")

    # Списки для обновления
    rows_to_update = []

    # Сопоставляем хосты
    for normalized_host in host_software_names:
        if normalized_host in server_index:
            server_data = server_index[normalized_host]

            # Получаем данные ПО
            software_names = host_software_names.get(normalized_host, [])
            software_versions_dict = host_software_versions.get(normalized_host, {})

            # Подготавливаем данные в зависимости от типа столбца
            software_data = prepare_column_data(software_names, SOFTWARE_COLUMN_TYPE)
            versions_data = prepare_column_data(
                software_versions_dict, VERSIONS_COLUMN_TYPE
            )

            # Получаем текущие значения
            current_software = server_data["current_software"]
            current_versions = server_data["current_versions"]

            # Проверяем изменения с учетом типа столбца
            software_changed = compare_values(
                current_software, software_data, SOFTWARE_COLUMN_TYPE
            )
            versions_changed = compare_values(
                current_versions, versions_data, VERSIONS_COLUMN_TYPE
            )

            if software_changed or versions_changed:
                updates = {}
                changes_log = []

                if software_changed:
                    updates["Софт факт"] = software_data

                    # Форматируем для логирования
                    if SOFTWARE_COLUMN_TYPE and isinstance(current_software, list):
                        old_val = sorted(current_software)
                    else:
                        old_val = (
                            str(current_software)[:50] if current_software else "пусто"
                        )

                    if SOFTWARE_COLUMN_TYPE:
                        new_val = (
                            sorted(software_data)
                            if isinstance(software_data, list)
                            else software_data
                        )
                    else:
                        new_val = str(software_data)[:50]

                    changes_log.append(f"ПО: {old_val} -> {new_val}")

                if versions_changed:
                    updates["Версия реальная"] = versions_data
                    old_val = (
                        str(current_versions)[:50] if current_versions else "пусто"
                    )
                    new_val = str(versions_data)[:50]
                    changes_log.append(f"Версии: {old_val} -> {new_val}")

                rows_to_update.append(
                    {
                        "_id": server_data["row"]["_id"],
                        "host": normalized_host,
                        "updates": updates,
                        "changes": changes_log,
                    }
                )

    # Выводим информацию о планируемых изменениях
    if rows_to_update:
        logger.info(f"\nНайдено {len(rows_to_update)} серверов для обновления:")
        for update in rows_to_update[:3]:
            logger.info(f"  Хост: {update['host']}")
            for change in update["changes"]:
                logger.info(f"    {change}")

        sort_issue_count = 0
        for update in rows_to_update:
            if "ПО:" in str(update["changes"]):
                for change in update["changes"]:
                    if "ПО:" in change and isinstance(
                        update["updates"].get("Софт факт"), list
                    ):
                        if isinstance(
                            server_index[update["host"]]["current_software"], list
                        ):
                            current_sorted = sorted(
                                server_index[update["host"]]["current_software"]
                            )
                            new_sorted = sorted(update["updates"]["Софт факт"])
                            if current_sorted == new_sorted:
                                sort_issue_count += 1

        if sort_issue_count > 0:
            logger.info(
                f"\nПримечание: {sort_issue_count} изменений только из-за разного порядка элементов"
            )

        if len(rows_to_update) > 3:
            logger.info(f"  ... и еще {len(rows_to_update) - 3} серверов")
    else:
        logger.info("\nНет серверов для обновления")

    # Если нет изменений
    if not rows_to_update:
        logger.info("\nНет изменений для синхронизации.")
        return

    # Запрос подтверждения у пользователя
    # try:
    #     user_input = input(f"\nВыполнить синхронизацию ({len(rows_to_update)} изменений) [y/N]: ").strip().lower()
    # except EOFError:
    #     user_input = 'n'
    #
    # if user_input != 'y':
    #     logger.info("Синхронизация отменена.")
    #     return
    #
    # Выполняем обновления
    logger.info("\nНачинается обновление серверов...")
    updated_count = 0

    for update in rows_to_update:
        try:
            base.update_row(SERVERS_TABLE, update["_id"], update["updates"])
            updated_count += 1
            logger.debug(f"Обновлен хост: {update['host']}")
        except Exception as e:
            logger.error(f"Ошибка при обновлении хоста {update['host']}: {e}")

    logger.info(f"\nСинхронизация завершена.")
    logger.info(f"  - Обновлено серверов: {updated_count}/{len(rows_to_update)}")
    logger.info(
        f"  - Тип столбца 'Софт факт': {'Множественный выбор' if SOFTWARE_COLUMN_TYPE else 'Текстовый'}"
    )
    logger.info(
        f"  - Тип столбца 'Версия реальная': {'Множественный выбор' if VERSIONS_COLUMN_TYPE else 'Текстовый'}"
    )


def main():
    """Основная функция скрипта."""
    logger.info("=" * 60)
    logger.info(
        f"Начало синхронизации информации о ПО и версиях: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("=" * 60)

    # 1. Получаем информацию о ПО и версиях по хостам
    host_software_names, host_software_versions = get_software_by_host()

    if not host_software_names:
        logger.warning("Не получено данных о ПО")
        return

    # 2. Синхронизируем в таблицу серверов
    sync_software_to_servers(host_software_names, host_software_versions)


if __name__ == "__main__":
    main()
