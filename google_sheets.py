import gspread
import logging
from oauth2client.service_account import ServiceAccountCredentials
from config import GOOGLE_SHEETS_CONFIG, GOOGLE_SHEETS_MAIN_CONFIG
from datetime import date, datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_google_sheet(data: list[dict]):
    """
    Авторизуется в Google Sheets и обновляет данные на листе,
    сохраняя существующее форматирование таблицы.
    Ищет строки по дате и обновляет их. Если дата не найдена,
    добавляет новую строку в конец таблицы, наследуя форматирование.

    Args:
        data: Полный список словарей с данными для загрузки.
    """
    try:
        logging.info("Авторизация в Google Sheets...")
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            GOOGLE_SHEETS_CONFIG['credentials_file'], scope
        )
        client = gspread.authorize(creds)

        logging.info(f"Открытие таблицы '{GOOGLE_SHEETS_CONFIG['spreadsheet_name']}'...")
        spreadsheet = client.open(GOOGLE_SHEETS_CONFIG['spreadsheet_name'])
        sheet = spreadsheet.worksheet(GOOGLE_SHEETS_CONFIG['worksheet_name'])
        
        logging.info("Получение существующих данных из таблицы...")
        try:
            sheet_values = sheet.get_all_values()
        except gspread.exceptions.GSpreadException as e:
            logging.warning(f"Не удалось прочитать лист (возможно, он пуст): {e}")
            sheet_values = []

        # 1. Подготавливаем новые данные
        header = ['Дата', 'Изделия', 'Раздвижки', 'МС', 'СП и стекла', 'Сэндвичи', 'Подоконники', 'Железо']
        processed_new_data = []
        for row in data:
            processed_row = {}
            for key, value in row.items():
                # Приводим ключи БД к названиям столбцов в таблице
                new_key = {'PRODDATE': 'Дата', 'QTY_IZD_PVH': 'Изделия', 'QTY_RAZDV': 'Раздвижки', 'QTY_MOSNET': 'МС', 'QTY_GLASS_PACKS': 'СП и стекла', 'QTY_SANDWICHES': 'Сэндвичи', 'QTY_WINDOWSILLS': 'Подоконники', 'QTY_IRON': 'Железо'}.get(key, key)
                if isinstance(value, (date, datetime)):
                    processed_row[new_key] = value.strftime('%d.%m.%Y')
                else:
                    processed_row[new_key] = value
            processed_new_data.append(processed_row)

        if not processed_new_data and not sheet_values:
            logging.info("Нет ни существующих, ни новых данных. Лист оставлен пустым.")
            return

        # Если лист пуст, просто вставляем все данные с заголовком
        if not sheet_values:
            logging.info("Лист пуст. Вставляем все данные с заголовком.")
            
            now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
            sheet.update('F1', [[f"Последнее обновление: {now}"]], value_input_option='USER_ENTERED')
            
            rows_to_insert = [header]
            
            for i, row_dict in enumerate(processed_new_data):
                row_values = [row_dict.get(h, '') for h in header]
                rows_to_insert.append(row_values)
            
            sheet.update('A2', rows_to_insert, value_input_option='USER_ENTERED')
            logging.info("Данные успешно загружены.")
            return

        # Если лист не пуст, выполняем обновление/добавление
        current_header = [str(h).strip() for h in sheet_values[1]] if len(sheet_values) > 1 else []
        try:
            date_column_index = current_header.index('Дата')
        except ValueError:
            logging.error("На листе в строке 2 отсутствует столбец 'Дата'. Невозможно выполнить обновление.")
            return

        # Создаем карту существующих дат и их номеров строк (1-based index)
        date_to_row_map = {row[date_column_index]: i for i, row in enumerate(sheet_values[2:], start=3)}

        updates_batch = []
        new_rows_to_insert = []

        for row_dict in processed_new_data:
            date_str = row_dict.get('Дата')
            if not date_str:
                continue

            # Собираем значения в том порядке, как они в заголовке на листе
            row_values = [row_dict.get(h, '') for h in current_header]

            if date_str in date_to_row_map:
                row_number = date_to_row_map[date_str]
                
                updates_batch.append({
                    'range': f'A{row_number}:{chr(ord("A")+len(current_header)-1)}{row_number}',
                    'values': [row_values]
                })
            else:
                # Если такой даты нет, это новая строка
                # Проверяем, чтобы эта дата не была уже в списке на добавление
                if date_str not in [r[date_column_index] for r in new_rows_to_insert]:
                     new_rows_to_insert.append(row_values)
        
        if updates_batch:
            logging.info(f"Обновление {len(updates_batch)} существующих строк...")
            sheet.batch_update(updates_batch, value_input_option='USER_ENTERED')

        if new_rows_to_insert:
            # Сортируем новые строки по дате перед вставкой
            try:
                new_rows_to_insert.sort(key=lambda r: datetime.strptime(r[date_column_index], '%d.%m.%Y'))
            except (ValueError, IndexError):
                logging.warning("Не удалось отсортировать новые строки перед вставкой.")

            logging.info(f"Добавление {len(new_rows_to_insert)} новых строк в конец таблицы...")
            # Вставляем строки после последней существующей строки, наследуя форматирование
            sheet.insert_rows(
                new_rows_to_insert,
                row=len(sheet_values) + 1,
                value_input_option='USER_ENTERED',
                inherit_from_before=True
            )
            
        # Отображаем только записи в окне: от 2 дней до сегодня и +5 дней
        # Реализуем через скрытие строк вне окна, чтобы избежать конфликтов базового фильтра
        try:
            logging.info("Применение окна отображения по дате (−2 до +5 дней)...")

            # Перечитываем данные после всех изменений, чтобы получить актуальные строки
            latest_values = sheet.get_all_values()
            if not latest_values:
                logging.info("Лист пуст после обновления — нечего фильтровать.")
            else:
                current_header = [str(h).strip() for h in latest_values[1]] if len(latest_values) > 1 else []
                try:
                    date_column_index = current_header.index('Дата')
                except ValueError:
                    logging.error("Столбец 'Дата' не найден — пропускаю применение окна отображения.")
                    date_column_index = None

                if date_column_index is not None:
                    # Границы окна
                    today = date.today()
                    start_date = today - timedelta(days=2)
                    end_date = today + timedelta(days=5)

                    # Собираем индексы строк (0-based в API; 1-я строка — заголовок) вне окна
                    rows_outside_window_zero_based = []
                    for row_1_based, row in enumerate(latest_values[2:], start=3):
                        raw_date = row[date_column_index] if date_column_index < len(row) else ''
                        try:
                            row_date = datetime.strptime(raw_date, '%d.%m.%Y').date()
                            in_window = (start_date <= row_date <= end_date)
                        except Exception:
                            # Если дата не парсится — скрываем
                            in_window = False

                        if not in_window:
                            # Преобразуем в 0-based индекс строки для API
                            rows_outside_window_zero_based.append(row_1_based - 1)

                    # Сначала показываем все строки (снимаем скрытие)
                    sheet_id = sheet.id if hasattr(sheet, 'id') else sheet._properties.get('sheetId')
                    total_rows = len(latest_values)

                    requests = []
                    if total_rows > 1:
                        requests.append({
                            'updateDimensionProperties': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'dimension': 'ROWS',
                                    'startIndex': 2,   # пропускаем заголовок и инфо
                                    'endIndex': total_rows
                                },
                                'properties': {
                                    'hiddenByUser': False
                                },
                                'fields': 'hiddenByUser'
                            }
                        })

                    # Группируем внеоконные строки в непрерывные диапазоны для минимизации запросов
                    def group_contiguous(indices: list[int]) -> list[tuple[int, int]]:
                        if not indices:
                            return []
                        indices.sort()
                        ranges = []
                        start = prev = indices[0]
                        for idx in indices[1:]:
                            if idx == prev + 1:
                                prev = idx
                                continue
                            ranges.append((start, prev + 1))  # end exclusive
                            start = prev = idx
                        ranges.append((start, prev + 1))
                        return ranges

                    hide_ranges_zero_based = group_contiguous(rows_outside_window_zero_based)
                    for start_idx, end_idx in hide_ranges_zero_based:
                        # Не скрываем заголовок; start_idx >= 2 гарантированно
                        if start_idx >= end_idx:
                            continue
                        requests.append({
                            'updateDimensionProperties': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'dimension': 'ROWS',
                                    'startIndex': start_idx,
                                    'endIndex': end_idx
                                },
                                'properties': {
                                    'hiddenByUser': True
                                },
                                'fields': 'hiddenByUser'
                            }
                        })

                    if requests:
                        spreadsheet.batch_update({'requests': requests})
                        logging.info(
                            "Окно отображения применено: показаны даты от %s до %s, скрыто диапазонов: %d",
                            start_date.strftime('%d.%m.%Y'), end_date.strftime('%d.%m.%Y'),
                            max(0, len(requests) - 1) if total_rows > 1 else 0
                        )
        except Exception as e:
            logging.error(f"Произошла ошибка при применении окна отображения: {e}")

        try:
            logging.info("Обновление времени последнего обновления в ячейке F1...")
            now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
            sheet.update('F1', [[f"Последнее обновление: {now}"]])
            logging.info("Время последнего обновления успешно записано в F1.")
        except Exception as e:
            logging.error(f"Не удалось обновить ячейку F1: {e}")

        # Применяем форматирование: шрифт 14 жирный для всей таблицы
        try:
            logging.info("Применение форматирования шрифта (14, жирный)...")
            sheet_id = sheet.id if hasattr(sheet, 'id') else sheet._properties.get('sheetId')
            
            # Перечитываем данные для получения актуального количества строк
            latest_values = sheet.get_all_values()
            total_rows = len(latest_values)
            
            format_requests = []
            
            # Применяем шрифт 14 и жирный ко всей таблице
            if total_rows > 0:
                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,
                            'endRowIndex': total_rows
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'fontSize': 14,
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.textFormat.fontSize,userEnteredFormat.textFormat.bold'
                    }
                })
            
            # Сначала убираем зеленое выделение со всех строк (кроме заголовка)
            # Сбрасываем фон на белый для всех строк с данными (первые 4 столбца)
            if total_rows > 2:
                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,  # С третьей строки 
                            'endRowIndex': total_rows,
                            'startColumnIndex': 0,  # Столбец A
                            'endColumnIndex': 8      # До столбца H включительно
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 1.0,
                                    'green': 1.0,
                                    'blue': 1.0
                                },
                                'textFormat': {
                                    'fontSize': 14,
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.fontSize,userEnteredFormat.textFormat.bold'
                    }
                })
            
            # Находим строку с текущей датой и выделяем только первые 6 ячеек светло-зеленым
            today_str = date.today().strftime('%d.%m.%Y')
            current_header = [str(h).strip() for h in latest_values[1]] if len(latest_values) > 1 else []
            
            try:
                date_column_index = current_header.index('Дата')
                for row_idx, row in enumerate(latest_values[2:], start=2):
                    if row[date_column_index] == today_str:
                        # Выделяем только первые 6 ячейки строки светло-зеленым цветом
                        format_requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': row_idx,
                                    'endRowIndex': row_idx + 1,
                                    'startColumnIndex': 0,  # Столбец A
                                    'endColumnIndex': 8      # До столбца H включительно
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'backgroundColor': {
                                            'red': 0.85,
                                            'green': 0.92,
                                            'blue': 0.83
                                        },
                                        'textFormat': {
                                            'fontSize': 14,
                                            'bold': True
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.fontSize,userEnteredFormat.textFormat.bold'
                            }
                        })
                        logging.info(f"Найдена и выделена строка с текущей датой: {today_str} (строка {row_idx + 1}, столбцы A-H)")
                        break
            except (ValueError, IndexError):
                logging.warning("Столбец 'Дата' не найден для выделения текущего дня.")
            
            if format_requests:
                spreadsheet.batch_update({'requests': format_requests})
                logging.info("Форматирование успешно применено.")
        except Exception as e:
            logging.error(f"Ошибка при применении форматирования: {e}")

        logging.info("Обновление данных в Google Sheets завершено.")

    except FileNotFoundError:
        logging.error(f"Файл {GOOGLE_SHEETS_CONFIG['credentials_file']} не найден. "
                      f"Пожалуйста, убедитесь, что он находится в корневом каталоге проекта.")
    except Exception as e:
        logging.error(f"Произошла ошибка при работе с Google Sheets: {e}")

def update_google_sheet_by_order(data: list[dict]):
    """
    Авторизуется в Google Sheets и обновляет данные на листе "Расшифр по заказам".
    """
    try:
        logging.info("Авторизация в Google Sheets для обновления данных по заказам...")
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            GOOGLE_SHEETS_CONFIG['credentials_file'], scope
        )
        client = gspread.authorize(creds)

        logging.info(f"Открытие таблицы '{GOOGLE_SHEETS_CONFIG['spreadsheet_name']}'...")
        spreadsheet = client.open(GOOGLE_SHEETS_CONFIG['spreadsheet_name'])
        sheet = spreadsheet.worksheet(GOOGLE_SHEETS_CONFIG['worksheet_name_by_order'])
        
        logging.info("Получение существующих данных из таблицы по заказам...")
        try:
            sheet_values = sheet.get_all_values()
        except gspread.exceptions.GSpreadException as e:
            logging.warning(f"Не удалось прочитать лист (возможно, он пуст): {e}")
            sheet_values = []

        header = ['Дата', 'ЗАКАЗ', 'Изделия', 'Раздвижки', 'МС', 'СП и стекла', 'Сэндвичи', 'Подоконники', 'Железо']
        processed_new_data = []
        for row in data:
            processed_row = {}
            for key, value in row.items():
                new_key = {'PRODDATE': 'Дата', 'ORDERNO': 'ЗАКАЗ', 'QTY_IZD_PVH': 'Изделия', 'QTY_RAZDV': 'Раздвижки', 'QTY_MOSNET': 'МС', 'QTY_GLASS_PACKS': 'СП и стекла', 'QTY_SANDWICHES': 'Сэндвичи', 'QTY_WINDOWSILLS': 'Подоконники', 'QTY_IRON': 'Железо'}.get(key, key)
                if isinstance(value, (date, datetime)):
                    processed_row[new_key] = value.strftime('%d.%m.%Y')
                else:
                    processed_row[new_key] = value
            processed_new_data.append(processed_row)

        if not processed_new_data and not sheet_values:
            logging.info("Нет ни существующих, ни новых данных. Лист по заказам оставлен пустым.")
            return

        if not sheet_values:
            logging.info("Лист по заказам пуст. Вставляем все данные с заголовком.")
            now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
            sheet.update('F1', [[f"Последнее обновление: {now}"]], value_input_option='USER_ENTERED')
            rows_to_insert = [header]
            for row_dict in processed_new_data:
                rows_to_insert.append([row_dict.get(h, 0) for h in header])
            sheet.update('A2', rows_to_insert, value_input_option='USER_ENTERED')
            logging.info("Данные по заказам успешно загружены.")
            return

        current_header = [str(h).strip() for h in sheet_values[1]] if len(sheet_values) > 1 else []
        try:
            date_column_index = current_header.index('Дата')
            order_column_index = current_header.index('ЗАКАЗ')
        except ValueError as e:
            logging.error(f"На листе '{GOOGLE_SHEETS_CONFIG['worksheet_name_by_order']}' отсутствует обязательный столбец: {e}. Невозможно выполнить обновление.")
            return

        # Composite key: (date, order) -> row_number
        existing_data_map = {(row[date_column_index], str(row[order_column_index])): i for i, row in enumerate(sheet_values[2:], start=3)}

        updates_batch = []
        new_rows_to_insert = []

        for row_dict in processed_new_data:
            date_str = row_dict.get('Дата')
            order_str = str(row_dict.get('ЗАКАЗ'))
            if not date_str or not order_str:
                continue
            
            row_values = [row_dict.get(h, 0) for h in current_header]

            composite_key = (date_str, order_str)
            if composite_key in existing_data_map:
                row_number = existing_data_map[composite_key]
                updates_batch.append({
                    'range': f'A{row_number}:{chr(ord("A")+len(current_header)-1)}{row_number}',
                    'values': [row_values]
                })
            else:
                new_rows_to_insert.append(row_values)
        
        if updates_batch:
            logging.info(f"Обновление {len(updates_batch)} существующих строк в листе по заказам...")
            sheet.batch_update(updates_batch, value_input_option='USER_ENTERED')

        if new_rows_to_insert:
            logging.info(f"Добавление {len(new_rows_to_insert)} новых строк в конец таблицы по заказам...")
            sheet.insert_rows(
                new_rows_to_insert,
                row=len(sheet_values) + 1,
                value_input_option='USER_ENTERED'
            )
        
        try:
            logging.info("Обновление времени последнего обновления в ячейке F1...")
            now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
            sheet.update('F1', [[f"Последнее обновление: {now}"]])
        except Exception as e:
            logging.error(f"Не удалось обновить ячейку F1: {e}")

        # Применяем фильтрацию и форматирование
        try:
            logging.info("Применение форматирования и фильтрации для листа по заказам...")
            sheet_id = sheet.id if hasattr(sheet, 'id') else sheet._properties.get('sheetId')
            
            latest_values = sheet.get_all_values()
            total_rows = len(latest_values)
            if total_rows < 2:
                logging.info("Недостаточно строк для форматирования или фильтрации.")
                return

            requests = []
            
            # 1. Показываем все строки (снимаем предыдущие фильтры)
            requests.append({
                'updateDimensionProperties': {
                    'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 2},
                    'properties': {'hiddenByUser': False},
                    'fields': 'hiddenByUser'
                }
            })

            # 2. Применяем форматирование ко всем данным
            # Форматируем ячейку F1
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0, 'endRowIndex': 1,
                        'startColumnIndex': 5, 'endColumnIndex': 6
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {'fontSize': 14, 'bold': True}
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat(fontSize,bold)'
                }
            })
            # Шрифт 14, жирный
            requests.append({
                'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 1, 'endRowIndex': total_rows},
                    'cell': {'userEnteredFormat': {'textFormat': {'fontSize': 14, 'bold': True}}},
                    'fields': 'userEnteredFormat.textFormat(fontSize,bold)'
                }
            })
            # Сброс фона на белый для всех строк данных
            requests.append({
                'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': 2, 'endRowIndex': total_rows, 'startColumnIndex': 0, 'endColumnIndex': 9},
                    'cell': {'userEnteredFormat': {'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}}},
                    'fields': 'userEnteredFormat.backgroundColor'
                }
            })

            # 3. Находим строки НЕ за сегодня для скрытия и строки ЗА сегодня для выделения
            current_header = [str(h).strip() for h in latest_values[1]]
            date_column_index = current_header.index('Дата')
            today_str = date.today().strftime('%d.%m.%Y')
            
            rows_to_hide_indices = []
            today_rows_indices = []

            for i, row in enumerate(latest_values):
                if i < 2: continue  # Пропускаем заголовки
                row_date_str = row[date_column_index] if date_column_index < len(row) else ''
                if row_date_str == today_str:
                    today_rows_indices.append(i)
                else:
                    rows_to_hide_indices.append(i)
            
            # Выделяем сегодняшние строки зеленым
            for row_idx in today_rows_indices:
                requests.append({
                    'repeatCell': {
                        'range': {'sheetId': sheet_id, 'startRowIndex': row_idx, 'endRowIndex': row_idx + 1, 'startColumnIndex': 0, 'endColumnIndex': 9},
                        'cell': {'userEnteredFormat': {'backgroundColor': {'red': 0.85, 'green': 0.92, 'blue': 0.83}}},
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })

            # Группируем индексы для скрытия в диапазоны
            def group_contiguous(indices: list[int]) -> list[tuple[int, int]]:
                if not indices: return []
                indices.sort()
                ranges, start = [], indices[0]
                for i in range(1, len(indices)):
                    if indices[i] != indices[i-1] + 1:
                        ranges.append((start, indices[i-1] + 1))
                        start = indices[i]
                ranges.append((start, indices[-1] + 1))
                return ranges

            # Скрываем строки не за сегодня
            for start_idx, end_idx in group_contiguous(rows_to_hide_indices):
                requests.append({
                    'updateDimensionProperties': {
                        'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': start_idx, 'endIndex': end_idx},
                        'properties': {'hiddenByUser': True},
                        'fields': 'hiddenByUser'
                    }
                })

            if requests:
                spreadsheet.batch_update({'requests': requests})
                logging.info(f"Форматирование применено. Показаны строки только за {today_str}. Скрыто {len(rows_to_hide_indices)} строк.")
            else:
                logging.info("Нечего форматировать или фильтровать.")

        except Exception as e:
            logging.error(f"Произошла ошибка при применении форматирования и фильтрации: {e}", exc_info=True)

        logging.info("Обновление данных в Google Sheets по заказам завершено.")

    except FileNotFoundError:
        logging.error(f"Файл {GOOGLE_SHEETS_CONFIG['credentials_file']} не найден.")
    except Exception as e:
        logging.error(f"Произошла ошибка при работе с Google Sheets: {e}")


def update_google_sheet_orders(data: list[dict]):
    """
    Обновляет данные на листе "Заказы" в основной таблице.
    Находит строку по номеру заказа (столбец B) и обновляет нужные поля.
    Если номер заказа не найден, пропускает эту запись.

    Args:
        data: Список словарей с данными из БД (с группировкой по заказам).
    """
    try:
        logging.info("Авторизация в Google Sheets для обновления листа 'Заказы'...")
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            GOOGLE_SHEETS_MAIN_CONFIG['credentials_file'], scope
        )
        client = gspread.authorize(creds)

        logging.info(f"Открытие таблицы по ID '{GOOGLE_SHEETS_MAIN_CONFIG['spreadsheet_id']}'...")
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_MAIN_CONFIG['spreadsheet_id'])
        sheet = spreadsheet.worksheet(GOOGLE_SHEETS_MAIN_CONFIG['worksheet_name_orders'])

        logging.info("Получение существующих данных из листа 'Заказы'...")
        try:
            sheet_values = sheet.get_all_values()
        except gspread.exceptions.GSpreadException as e:
            logging.warning(f"Не удалось прочитать лист (возможно, он пуст): {e}")
            sheet_values = []

        if not sheet_values or len(sheet_values) < 2:
            logging.error("Лист 'Заказы' пуст или не содержит заголовков. Невозможно выполнить обновление.")
            return

        # Получаем заголовки из первой строки
        header = [str(h).strip() for h in sheet_values[0]]
        logging.info(f"Заголовки таблицы: {header}")
        logging.info(f"Заголовок столбца G (индекс 6): '{header[6] if len(header) > 6 else 'НЕТ'}'")

        # Определяем индексы столбцов (столбцы считаются с 0)
        try:
            # Пробуем найти столбец с номером заказа по разным вариантам названия
            order_col_idx = None
            for possible_name in ['номер', 'Номер', 'Номер заказа', 'ном ер']:
                try:
                    order_col_idx = header.index(possible_name)
                    logging.info(f"Найден столбец с номером заказа: '{possible_name}' (индекс {order_col_idx})")
                    break
                except ValueError:
                    continue

            if order_col_idx is None:
                raise ValueError("Не найден столбец с номером заказа. Проверьте заголовки.")

            # Ищем остальные столбцы, используя точные названия из заголовков
            proddate_col_idx = None
            qty_izd_col_idx = None
            qty_glass_col_idx = None
            qty_razdv_col_idx = None
            qty_mosnet_col_idx = None
            qty_iron_col_idx = None
            qty_windowsills_col_idx = None
            qty_sandwiches_col_idx = None
            totalprice_col_idx = None

            for idx, col_name in enumerate(header):
                if col_name == 'Дата произв-ва':
                    proddate_col_idx = idx
                elif col_name == 'Кол-во изд.':
                    qty_izd_col_idx = idx
                elif col_name == 'кол-во зап.':
                    qty_glass_col_idx = idx
                elif col_name == 'сумма заказа':
                    totalprice_col_idx = idx
                elif col_name == 'Раздв':
                    qty_razdv_col_idx = idx
                elif col_name == 'М/С':
                    qty_mosnet_col_idx = idx
                elif col_name == 'Изд из мет.':
                    qty_iron_col_idx = idx
                elif col_name == 'Подок-ки':
                    qty_windowsills_col_idx = idx
                elif col_name == 'Сендв':
                    qty_sandwiches_col_idx = idx

            # Проверяем, что все столбцы найдены
            missing_columns = []
            if proddate_col_idx is None:
                missing_columns.append('Дата произв-ва')
            if qty_izd_col_idx is None:
                missing_columns.append('Кол-во изд.')
            if qty_glass_col_idx is None:
                missing_columns.append('кол-во зап.')
            if totalprice_col_idx is None:
                missing_columns.append('сумма заказа')
            if qty_razdv_col_idx is None:
                missing_columns.append('Раздв')
            if qty_mosnet_col_idx is None:
                missing_columns.append('М/С')
            if qty_iron_col_idx is None:
                missing_columns.append('Изд из мет.')
            if qty_windowsills_col_idx is None:
                missing_columns.append('Подок-ки')
            if qty_sandwiches_col_idx is None:
                missing_columns.append('Сендв')

            if missing_columns:
                raise ValueError(f"Не найдены столбцы: {', '.join(missing_columns)}")

            # Логируем найденные индексы столбцов
            logging.info(f"Индексы столбцов: order={order_col_idx}, proddate={proddate_col_idx}, "
                        f"qty_izd={qty_izd_col_idx}, qty_glass={qty_glass_col_idx}, totalprice={totalprice_col_idx}, "
                        f"qty_razdv={qty_razdv_col_idx}, qty_mosnet={qty_mosnet_col_idx}, qty_iron={qty_iron_col_idx}, "
                        f"qty_windowsills={qty_windowsills_col_idx}, qty_sandwiches={qty_sandwiches_col_idx}")
        except ValueError as e:
            logging.error(f"На листе 'Заказы' отсутствует обязательный столбец: {e}. Невозможно выполнить обновление.")
            return

        # Создаем карту: номер заказа -> индекс строки (1-based для API)
        order_to_row_map = {}
        for i, row in enumerate(sheet_values[1:], start=2):  # Начинаем со строки 2 (индекс 1 - заголовок)
            if order_col_idx < len(row):
                order_number = str(row[order_col_idx]).strip()
                if order_number:
                    order_to_row_map[order_number] = i

        logging.info(f"Найдено {len(order_to_row_map)} заказов в таблице.")

        # Функция для преобразования индекса столбца в буквенное обозначение (A, B, ..., Z, AA, AB, ...)
        def col_idx_to_letter(idx):
            """Преобразует индекс столбца (0-based) в буквенное обозначение."""
            result = ""
            idx += 1  # Переводим в 1-based
            while idx > 0:
                idx -= 1
                result = chr(ord('A') + (idx % 26)) + result
                idx //= 26
            return result

        # Подготавливаем batch-обновления
        # Группируем соседние столбцы для уменьшения количества запросов
        updates_batch = []
        updated_count = 0
        skipped_count = 0

        for row_dict in data:
            order_no = str(row_dict.get('ORDERNO', '')).strip()
            if not order_no:
                logging.warning(f"Пропущен заказ с пустым номером: {row_dict}")
                skipped_count += 1
                continue

            if order_no not in order_to_row_map:
                logging.warning(f"Заказ №{order_no} не найден в таблице (дата: {row_dict.get('PRODDATE')}), пропускаем.")
                skipped_count += 1
                continue

            row_number = order_to_row_map[order_no]

            # Формируем данные для обновления
            # Дата производства
            proddate = row_dict.get('PRODDATE')
            if isinstance(proddate, (date, datetime)):
                proddate_str = proddate.strftime('%d.%m.%Y')
            else:
                proddate_str = str(proddate) if proddate else ''

            # Количественные показатели (если нет данных - ставим 0)
            qty_izd = row_dict.get('QTY_IZD_PVH', 0) or 0
            qty_glass = row_dict.get('QTY_GLASS_PACKS', 0) or 0
            # Преобразуем Decimal в float для JSON сериализации
            totalprice_raw = row_dict.get('TOTALPRICE', 0) or 0
            totalprice = float(totalprice_raw) if totalprice_raw else 0
            qty_razdv = row_dict.get('QTY_RAZDV', 0) or 0
            qty_mosnet = row_dict.get('QTY_MOSNET', 0) or 0
            qty_iron = row_dict.get('QTY_IRON', 0) or 0
            qty_windowsills = row_dict.get('QTY_WINDOWSILLS', 0) or 0
            qty_sandwiches = row_dict.get('QTY_SANDWICHES', 0) or 0

            # Отладочное логирование для первых 5 заказов
            if updated_count < 5:
                logging.info(f"Заказ №{order_no}: PRODDATE={proddate_str}, TOTALPRICE={totalprice}, QTY_IRON={qty_iron}")
                logging.info(f"  Все ключи row_dict: {list(row_dict.keys())}")
                logging.info(f"  Значение TOTALPRICE из row_dict: {row_dict.get('TOTALPRICE', 'ОТСУТСТВУЕТ')}")

            # Обновляем каждый столбец отдельно, используя динамические индексы
            updates_batch.append({
                'range': f'{col_idx_to_letter(qty_izd_col_idx)}{row_number}',
                'values': [[qty_izd]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(qty_glass_col_idx)}{row_number}',
                'values': [[qty_glass]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(totalprice_col_idx)}{row_number}',
                'values': [[totalprice]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(proddate_col_idx)}{row_number}',
                'values': [[proddate_str]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(qty_razdv_col_idx)}{row_number}',
                'values': [[qty_razdv]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(qty_mosnet_col_idx)}{row_number}',
                'values': [[qty_mosnet]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(qty_iron_col_idx)}{row_number}',
                'values': [[qty_iron]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(qty_windowsills_col_idx)}{row_number}',
                'values': [[qty_windowsills]]
            })
            updates_batch.append({
                'range': f'{col_idx_to_letter(qty_sandwiches_col_idx)}{row_number}',
                'values': [[qty_sandwiches]]
            })

            updated_count += 1

        if updates_batch:
            logging.info(f"Обновление {updated_count} заказов ({len(updates_batch)} запросов)...")
            # Batch update может принимать максимум 500 запросов за раз
            # Разбиваем на части если нужно
            batch_size = 500
            for i in range(0, len(updates_batch), batch_size):
                batch_chunk = updates_batch[i:i + batch_size]
                sheet.batch_update(batch_chunk, value_input_option='USER_ENTERED')
                logging.info(f"Обновлено {min(i + batch_size, len(updates_batch))}/{len(updates_batch)} запросов.")

        # Применяем числовое форматирование к числовым столбцам
        try:
            logging.info("Применение форматирования к числовым столбцам...")
            sheet_id = sheet.id if hasattr(sheet, 'id') else sheet._properties.get('sheetId')

            format_requests = []

            # Форматируем столбец "сумма заказа" как денежное значение с 2 знаками после запятой
            format_requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startColumnIndex': totalprice_col_idx,
                        'endColumnIndex': totalprice_col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '#,##0.00'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

            # Форматируем все остальные столбцы с количествами как целые числа
            for col_idx in [qty_izd_col_idx, qty_glass_col_idx, qty_razdv_col_idx,
                           qty_mosnet_col_idx, qty_iron_col_idx, qty_windowsills_col_idx, qty_sandwiches_col_idx]:
                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })

            spreadsheet.batch_update({'requests': format_requests})
            logging.info("Форматирование применено: сумма заказа (денежное), остальные столбцы (целое число).")
        except Exception as e:
            logging.error(f"Ошибка при применении форматирования к столбцам: {e}")

        logging.info(f"Обновление завершено. Обновлено заказов: {updated_count}, Пропущено: {skipped_count}")

    except FileNotFoundError:
        logging.error(f"Файл {GOOGLE_SHEETS_MAIN_CONFIG['credentials_file']} не найден.")
    except Exception as e:
        logging.error(f"Произошла ошибка при работе с Google Sheets (лист 'Заказы'): {e}", exc_info=True)


if __name__ == '__main__':
    # Пример использования:
    # Для запуска этого примера, убедитесь, что у вас есть credentials.json
    # и вы предоставили доступ сервисному аккаунту к вашей таблице.

    # Пример данных
    sample_data = [
        {'PRODDATE': date(2023, 10, 1), 'QTY_IZD_PVH': 10, 'QTY_RAZDV': 5, 'QTY_MOSNET': 20},
        {'PRODDATE': date(2023, 10, 2), 'QTY_IZD_PVH': 12, 'QTY_RAZDV': 8, 'QTY_MOSNET': 22},
    ]

    update_google_sheet(sample_data)
    # print("Для тестирования этого модуля раскомментируйте вызов update_google_sheet "
    #       "и убедитесь в наличии credentials.json")
