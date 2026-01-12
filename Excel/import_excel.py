import pandas as pd
import os
import re
from sqlalchemy import create_engine, text

# 1.  Налаштування
conn_string = "postgresql://sigma:SigmaDB@10.0.0.2:5432/inverter"
engine = create_engine(conn_string)
folder_path = './excel_files'

# Отримуємо список дозволених реєстрів
allowed_registers = pd.read_sql('SELECT DISTINCT register_name FROM modbus_data', engine)
allowed_set = set(allowed_registers['register_name'].tolist())

def process_file(file_path):
# Зчитуємо файл (рядок 3 — заголовок)
    df = pd.read_excel(file_path, skiprows=2)

    # Створюємо словник для перейменування колонок
    # { 'OldName/123': 'OldName', 'Time': 'Time' ... }
    rename_map = {}
    for col in df.columns:
        # Регулярний вираз: шукаємо "/" та 3 цифри в кінці рядка ($)
        new_name = re.sub(r'/\d{3}$', '', str(col))
        # Регулярний вираз: шукаємо "/" та 3 цифри, дефіс, 3 цифри в кінці рядка ($)
        new_name = re.sub(r'/\d{3}-\d{3}$', '', new_name)
        new_name = new_name.replace('℃', 'C')
        rename_map[col] = new_name
        #print(f"old name={str(col)}, new name={new_name}")

    # Перейменовуємо колонки в DataFrame
    df = df.rename(columns=rename_map)
    
    # Перевіряємо, які колонки з allowed_set відсутні в Excel
    excel_cols_set = set(df.columns)
    missing_cols = allowed_set - excel_cols_set - {'Time'}  # Time може бути в allowed_set, але це нормально
    if missing_cols:
        print(f"УВАГА: У файлі {os.path.basename(file_path)} відсутні наступні колонки з бази даних:")
        for col in sorted(missing_cols):
            print(f"  - {col}")
    
# Тепер фільтруємо, базуючись на вже чистих назвах
    cols_to_keep = [col for col in df.columns if col in allowed_set or col == "Time"]

    # Перевірка: чи не порожній список колонок?
    if len(cols_to_keep) <= 1: # Тільки 'Time' або взагалі нічого
        return pd.DataFrame()

    df = df[cols_to_keep]
    ##print(f"Рядків після вибору колонок: {len(df)}")

    # Конвертуємо всі значення даних у числові (int або float)
    # Пропускаємо значення, які не можна конвертувати
    data_cols = [col for col in df.columns if col != 'Time']
    if data_cols:
        for col in data_cols:
            # Конвертуємо колонку в numeric, неконвертовані значення стають NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Фільтруємо порожні рядки: видаляємо рядки з порожнім Time
    initial_count = len(df)
    df = df.dropna(subset=['Time'])
    # Видаляємо рядки де Time є порожнім рядком
    df = df[df['Time'].astype(str).str.strip() != '']
    ##print(f"Рядків після фільтрації Time: {len(df)} (видалено {initial_count - len(df)})")
    
    # Фільтруємо рядки де всі значення даних порожні
    data_cols = [col for col in df.columns if col != 'Time']
    if data_cols:
        before_all_empty = len(df)
        # Видаляємо рядки де всі значення NaN (неконвертовані)
        df = df.dropna(subset=data_cols, how='all')
        # Видаляємо рядки де всі значення дорівнюють 0
        has_non_zero = pd.Series([False] * len(df), index=df.index)
        for col in data_cols:
            has_non_zero = has_non_zero | (df[col].notna() & (df[col] != 0))
        df = df[has_non_zero]
        print(f"Рядків після фільтрації порожніх даних: {len(df)} (видалено {before_all_empty - len(df)})")

    # Трансформація у "довгий" формат
    print(f"Рядків перед melt: {len(df)}, колонок даних: {len(data_cols)}")
    df_melted = df.melt(id_vars=['Time'], var_name='register_name', value_name='value')
    print(f"Рядків після melt: {len(df_melted)}")

    # Сортуємо за register_name та Time для правильного визначення змін по кожній колонці
    df_melted = df_melted.sort_values(['register_name', 'Time']).reset_index(drop=True)

    # Фільтруємо: зберігаємо тільки значення які змінилися порівняно з попереднім рядком для того ж register_name
    before_change_filter = len(df_melted)
    # Для кожної групи register_name визначаємо, чи змінилося значення
    df_melted['prev_value'] = df_melted.groupby('register_name')['value'].shift(1)
    # Перший рядок для кожного register_name завжди зберігаємо, інші - тільки якщо значення змінилося
    is_first = df_melted.groupby('register_name').cumcount() == 0
    has_changed = (df_melted['value'] != df_melted['prev_value']) | is_first
    
    # Додаткова перевірка: якщо для register_name є тільки одне унікальне значення, зберігаємо рядок з мінімальним Time
    # (після сортування перший рядок вже має мінімальний Time, тому is_first вже покриває цей випадок,
    # але перевіряємо явно для надійності)
    unique_counts = df_melted.groupby('register_name')['value'].nunique()
    single_value_registers = unique_counts[unique_counts == 1].index
    for reg_name in single_value_registers:
        reg_mask = df_melted['register_name'] == reg_name
        if reg_mask.any():
            # Знаходимо індекс рядка з мінімальним Time для цього register_name
            reg_indices = df_melted[reg_mask].index
            min_time_idx = df_melted.loc[reg_indices, 'Time'].idxmin()
            has_changed.loc[min_time_idx] = True
    
    df_melted = df_melted[has_changed].drop(columns=['prev_value'])
    print(f"Рядків після фільтрації змін: {len(df_melted)} (видалено {before_change_filter - len(df_melted)})")

    # Обробка дати
    df_melted['timestamp'] = pd.to_datetime(df_melted['Time']).dt.tz_localize('UTC')
    # Конвертуємо timestamp в epoch (секунди з 1970-01-01)
    df_melted['id'] = (df_melted['timestamp'].astype('int64') // 10**9).astype('int64')

    df_melted['device_id'] = 1
    # Фільтруємо порожні значення: видаляємо NaN (неконвертовані значення)
    before_value_filter = len(df_melted)
    df_melted = df_melted.dropna(subset=['value'])
    # Видаляємо рядки де value дорівнює 0
    df_melted = df_melted[df_melted['value'] != 0]
    # Видаляємо рядки де timestamp не вдалося створити (невалідний Time)
    df_melted = df_melted.dropna(subset=['timestamp'])
    ##print(f"Рядків після фільтрації порожніх значень: {len(df_melted)} (видалено {before_value_filter - len(df_melted)})")

    return df_melted[['id', 'device_id', 'timestamp', 'register_name', 'value']]

# 2. Збір даних з усіх файлів у пам'ять
all_dfs = []
files_found = 0
for file in os.listdir(folder_path):
    if file.endswith(".xlsx"):
        print(f"Читання {file}...")
        try:
            df_processed = process_file(os.path.join(folder_path, file))
            ##print(f"Знайдено рядків після фільтрації: {len(df_processed)}")
            if not df_processed.empty:
                all_dfs.append(df_processed)
        except Exception as e:
            print(f"Помилка у файлі {file}: {e}")
if not all_dfs:
    print(f"УВАГА: Жодних даних не зібрано! Перевірено файлів: {files_found}")
    print("Перевірте, чи назви колонок в Excel збігаються з базою (реєстр: SELECT DISTINCT...)")
else:
    final_df = pd.concat(all_dfs, ignore_index=True)
    ##print(f"Загалом підготовлено до запису: {len(final_df)} рядків")

    # Запис у базу
    with engine.begin() as connection:
        # Використовуємо replace, щоб таблиця точно створилася з даними
        final_df.to_sql('temp_modbus_data', connection, if_exists='replace', index=False)
        ##print("Дані записані у temp_modbus_data")

        # Перевірка всередині тієї ж транзакції
        res = connection.execute(text("SELECT count(*) FROM temp_modbus_data")).fetchone()
        ##print(f"Перевірка: в таблиці temp_modbus_data зараз {res[0]} записів")


    print("Завантаження завершено!")
