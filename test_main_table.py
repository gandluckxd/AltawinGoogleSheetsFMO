"""
Тестовый скрипт для проверки подключения к основной таблице
ТАБЛИЦА УЧЕТА ЗАКАЗОВ (ID: 1pbz9K6uarZy-3oax9OGfyA6MxtDCNoI9noavlr1YhOc)
"""
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_main_table_connection():
    """
    Тестирует подключение к основной таблице и выводит информацию о ней.
    """
    try:
        # ID основной таблицы
        MAIN_TABLE_ID = '1pbz9K6uarZy-3oax9OGfyA6MxtDCNoI9noavlr1YhOc'
        CREDENTIALS_FILE = 'credentials.json'

        logging.info("=" * 60)
        logging.info("ТЕСТ ПОДКЛЮЧЕНИЯ К ОСНОВНОЙ ТАБЛИЦЕ")
        logging.info("=" * 60)

        # Авторизация
        logging.info("1. Авторизация в Google Sheets...")
        scope = [
            "https://spreadsheets.google.com/feeds",
            'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        logging.info("✓ Авторизация успешна")

        # Открытие таблицы
        logging.info(f"\n2. Открытие таблицы по ID: {MAIN_TABLE_ID}...")
        spreadsheet = client.open_by_key(MAIN_TABLE_ID)
        logging.info(f"✓ Таблица успешно открыта: '{spreadsheet.title}'")

        # Получение списка листов
        logging.info("\n3. Получение списка листов...")
        worksheets = spreadsheet.worksheets()
        logging.info(f"✓ Найдено листов: {len(worksheets)}")

        for i, ws in enumerate(worksheets, 1):
            logging.info(f"   {i}. '{ws.title}' (ID: {ws.id}, строк: {ws.row_count}, столбцов: {ws.col_count})")

        # Проверка наличия листа "Заказы"
        logging.info("\n4. Поиск листа 'Заказы'...")
        try:
            orders_sheet = spreadsheet.worksheet('Заказы')
            logging.info(f"✓ Лист 'Заказы' найден (ID: {orders_sheet.id})")

            # Получение заголовков
            logging.info("\n5. Чтение заголовков листа 'Заказы'...")
            try:
                all_values = orders_sheet.get_all_values()
                if all_values:
                    headers = all_values[0]
                    logging.info(f"✓ Заголовки (строка 1): {headers}")
                    logging.info(f"   Всего столбцов: {len(headers)}")
                    logging.info(f"   Всего строк с данными: {len(all_values)}")

                    # Проверяем наличие нужных столбцов
                    logging.info("\n6. Проверка наличия необходимых столбцов...")
                    required_columns = {
                        'Номер заказа': ['номер', 'Номер', 'Номер заказа', 'ном ер'],
                        'Дата производства': ['Дата произв-ва'],
                        'Количество изделий': ['Кол-во изд.'],
                        'Количество заполнений': ['кол-во зап.'],
                        'Сумма заказа': ['сумма заказа'],
                        'Раздвижки': ['Раздвижка'],
                        'Москитная сетка': ['М/С'],
                        'Изделия из металла': ['Изд из мет.'],
                        'Подоконники': ['Подок-ки'],
                        'Сэндвичи': ['Сендв']
                    }

                    found_columns = {}
                    missing_columns = []

                    for col_name, possible_names in required_columns.items():
                        found = False
                        for possible_name in possible_names:
                            if possible_name in headers:
                                idx = headers.index(possible_name)
                                found_columns[col_name] = (possible_name, idx)
                                logging.info(f"   ✓ {col_name}: найден как '{possible_name}' (столбец {chr(65+idx)}, индекс {idx})")
                                found = True
                                break
                        if not found:
                            missing_columns.append(col_name)
                            logging.warning(f"   ✗ {col_name}: НЕ НАЙДЕН (искали: {possible_names})")

                    # Итоговая информация
                    logging.info("\n" + "=" * 60)
                    logging.info("РЕЗУЛЬТАТ ТЕСТА:")
                    logging.info("=" * 60)
                    logging.info(f"Название таблицы: {spreadsheet.title}")
                    logging.info(f"ID таблицы: {MAIN_TABLE_ID}")
                    logging.info(f"Лист 'Заказы': НАЙДЕН")
                    logging.info(f"Всего строк: {len(all_values)}")
                    logging.info(f"Найдено столбцов: {len(found_columns)}/{len(required_columns)}")

                    if missing_columns:
                        logging.warning(f"\n⚠ ВНИМАНИЕ: Не найдены столбцы: {', '.join(missing_columns)}")
                        logging.warning("Обновление данных может работать некорректно!")
                    else:
                        logging.info("\n✓ ВСЕ НЕОБХОДИМЫЕ СТОЛБЦЫ НАЙДЕНЫ")
                        logging.info("✓ Таблица готова для работы!")

                    # Показываем несколько примеров данных
                    if len(all_values) > 1:
                        logging.info(f"\n7. Примеры данных (первые 3 строки после заголовка):")
                        for i, row in enumerate(all_values[1:4], start=2):
                            logging.info(f"   Строка {i}: {row[:5]}...")  # Первые 5 ячеек

                else:
                    logging.warning("✗ Лист 'Заказы' пустой!")

            except Exception as e:
                logging.error(f"✗ Ошибка при чтении данных листа: {e}")

        except gspread.exceptions.WorksheetNotFound:
            logging.error("✗ Лист 'Заказы' НЕ НАЙДЕН в таблице!")
            logging.info("\nДоступные листы:")
            for ws in worksheets:
                logging.info(f"   - '{ws.title}'")

        logging.info("\n" + "=" * 60)
        logging.info("ТЕСТ ЗАВЕРШЕН")
        logging.info("=" * 60)
        return True

    except FileNotFoundError:
        logging.error(f"✗ Файл {CREDENTIALS_FILE} не найден!")
        return False
    except gspread.exceptions.APIError as e:
        logging.error(f"✗ Ошибка API Google Sheets: {e}")
        logging.error("Возможные причины:")
        logging.error("  - Нет доступа к таблице для сервисного аккаунта")
        logging.error("  - Неверный ID таблицы")
        return False
    except Exception as e:
        logging.error(f"✗ Непредвиденная ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_main_table_connection()
