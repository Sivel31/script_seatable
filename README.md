# Скрипты на Python (MDM)

Репозиторий содержит набор Python-скриптов для автоматизации задач MDM: мониторинг ресурсов, сбор версий ПО и интеграция с внешними системами (Seatable, VictoriaMetrics).

## 📋 Обзор репозитория

| Скрипт / Папка                          | Описание                                                                    |
| :-------------------------------------- | :-------------------------------------------------------------------------- |
| [`recources_usage`](#recources_usage)   | Сбор метрик использования ресурсов (CPU, Disk) и отправка в VictoriaMetrics |
| [`seatable_servers`](#seatable_servers) | Интеграция с Seatable, заполнение и синхронизация таблицы "Серверы"         |
| [`soft_version`](#soft_version)         | Проверка версий установленного ПО на серверах                               |

---

## Полезные ссылки

- **Документация ДУД** (подключение к VictoriaMetrics): [Просмотр страницы](https://data.passport.local:8090/pages/viewpage.action?pageId=163053824)
- **GitLab ДИТ** (альтернативный репозиторий): [Перейти в репозиторий](https://data.passport.local:8220/ident/cvpz/devops/mdm_spo/-/tree/main/python_scripts)
- **Wiki krlb** (описание в confluence): [Просмотр страницы](https://wiki.krlb.ru/pages/viewpage.action?pageId=105454390)

---

## Подготовка окружения и запуск

Подготовка окружения
Для запуска скрипов необходимо в окружении (откуда будет запускаться скрипт) задать переменные: VICTORIA_TOKEN, API_SEATABLE_MDM, SEATABLE_SERVER

export VICTORIA_TOKEN="xxxxx
export API_SEATABLE_MDM="xxxxxxxxx"
export SEATABLE_SERVER="<https://sea.krlb.ru>"

Предварительно рекомендуется сверить название таблицы в переменной TABLE_NAME с наличием в SeaTable. Названия столбцов так же должны соответствовать.

Для запуска скриптов потребуется установка библиотек requests seatable-api urllib3

Для проверки наличия (возможно запускали ранее)

pip list | grep -E "(requests|seatable|urllib3)"
Если нужных библиотек нет, потребуется их установить. В современных версиях python установщик pip install может выдать ошибку при попытки прямой установки, потому один из способов установка в виртуальном окружении venv (или выбрать иные способы установки)

# Создать окружение

python3 -m venv venv

# Активировать его

source venv/bin/activate

# Установить зависимости

pip install requests seatable-api urllib3

# Запуск скрипта

python3 script.py или если запустить все скрипты uv run main.py
Если скрипт получает данные из VictoriaMetrics, из окружения должна быть доступна VPN - сеть
