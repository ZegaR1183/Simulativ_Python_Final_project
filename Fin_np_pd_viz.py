import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Отображение всех столбцов
pd.set_option('display.max_columns', None)
# Установка ширины вывода
pd.set_option('display.width', 1000)

# Чтение файлов и формирование датафреймов
df_orders = pd.read_excel('/home/zega_r/Desktop/Education/Simulative/Final_project /data/orders.xlsx')
df_products = pd.read_excel('/home/zega_r/Desktop/Education/Simulative/Final_project /data/products.xlsx')

# Объединение датафреймов
df = pd.merge(df_products, df_orders, on='product_id')

# Добавление столбцов прибыли и выручки
df['revenue'] = df['price'] * df['quantity']  # Выручка
df['cost'] = df['cost_price'] * df['quantity']  # Себестоимость
df['profit'] = df['revenue'] - df['cost']  # Прибыль

# Функция определения самой ходовой группы товара и отображение на графике
def popular_product_group(df):
    # группировка результата
    grouped_by_level_1 = df.groupby('level1')['quantity'].sum()

    # Сортировка по значению
    grouped_by_level_1 = grouped_by_level_1.sort_values(ascending=False)

    # Построение барчарта
    plt.figure(figsize=(12, 6))
    plt.bar(grouped_by_level_1.index, grouped_by_level_1.values, color='skyblue')

    # Добавляем заголовок и метки осей
    plt.title('Количество проданных позиций по категориям товаров')
    plt.xlabel('Категория')
    plt.ylabel('Количество проданных позиций')

    # Повернем метки на оси X, чтобы они были читаемыми, если их много
    plt.xticks(rotation=45, ha='right')

    # Показываем график
    plt.tight_layout()
    plt.show()

def sales_by_subcategories(df):
    # Группировка данных по категориям и подкатегориям
    grouped = df.groupby(['level1', 'level2'])['quantity'].sum().reset_index()
    grouped = grouped.sort_values(by='quantity', ascending=False)
    print(grouped)

    # Визуализация
    plt.figure(figsize=(24, 18))

    # Создание столбчатой диаграммы
    for level1 in grouped['level1'].unique():
        sub_data = grouped[grouped['level1'] == level1]
        plt.bar(sub_data['level2'], sub_data['quantity'], label=level1)

    # Добавляем заголовок и метки осей
    plt.title('Распределение проданных позиций по категориям и подкатегориям')
    plt.xlabel('Подкатегория')
    plt.ylabel('Количество проданных позиций')

    # Повернем метки на оси X для лучшей читаемости
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Категория')

    # Показываем график
    plt.tight_layout()
    plt.show()

# Cредний чек в заданную дату
specific_date = '2022-01-13'

def average_check(df_orders, specific_date):
    # Конвертация столбца с датами в формат datetime для фильтрации
    df_orders['accepted_at'] = pd.to_datetime(df_orders['accepted_at'])

    # Фильтрация данных по заданной дате
    daily_orders = df_orders[df_orders['accepted_at'].dt.date == pd.to_datetime(specific_date).date()].copy()

    # Сумма выручки для каждого заказа
    daily_orders['revenue'] = daily_orders['price'] * daily_orders['quantity']

    # Расчет общего количества заказов и общей суммы выручки за день
    total_revenue = daily_orders['revenue'].sum()
    total_orders = daily_orders['order_id'].nunique()

    # Вычисление среднего чека
    average_check = total_revenue / total_orders if total_orders != 0 else 0

    print(f"Средний чек на {specific_date}: {average_check:.2f} рублей")

# Доля промо в заданной категории
def promo_share_category(df, category_name):
    # Фильтрация по категории
    category_df = df[df['level1'] == category_name].copy()

    # Товары, проданные по промо
    category_df['is_promo'] = category_df['regular_price'] != category_df['price']

    # Суммарное количество проданных товаров и проданных по промо товаров
    total_quantity = category_df['quantity'].sum()
    promo_quantity = category_df[category_df['is_promo']]['quantity'].sum()

    # Доля промо
    promo_share = promo_quantity / total_quantity if total_quantity != 0 else 0

    # Вывод
    print(f"Доля товаров по промо в категории '{category_name}': {promo_share:.2%}")

    # Построение пайчарта
    labels = ['Промо', 'Не промо']
    sizes = [promo_quantity, total_quantity - promo_quantity]
    colors = ['skyblue', 'lightgrey']

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=140)
    plt.title(f"Доля промо в категории '{category_name}'")
    plt.axis('equal')  # Круг
    plt.show()

# Посчитать маржу по категориям
def calculate_margin_by_category(df):

    # Группировка данных по категориям
    category_profit = df.groupby('level1').agg(
        total_profit=pd.NamedAgg(column='profit', aggfunc='sum'),
        total_revenue=pd.NamedAgg(column='revenue', aggfunc='sum')
    ).reset_index()

    # Расчет процентной маржи
    category_profit['profit_margin_percent'] = (
                category_profit['total_profit'] / category_profit['total_revenue'] * 100).round(2)

    # Вывод таблицы маржи
    print(category_profit)

    # Построение барчартов
    plt.figure(figsize=(14, 6))
    plt.barh(category_profit['level1'], category_profit['total_profit'], color='skyblue')
    plt.title('Прибыль по категориям (руб)')
    plt.xlabel('Прибыль (руб)')
    plt.ylabel('Категория')
    plt.show()

    plt.figure(figsize=(14, 6))
    plt.barh(category_profit['level1'], category_profit['profit_margin_percent'], color='lightgreen')
    plt.title('Процентная маржа по категориям (%)')
    plt.xlabel('Маржа (%)')
    plt.ylabel('Категория')
    plt.show()

def abc_analysis(df):
    # # Вычисление выручки и количества по подкатегориям
    # df['revenue'] = df['price'] * df['quantity']

    # Вычисление выручки и количества по подкатегориям
    subcategory_stats = df.groupby('level2').agg(
        total_quantity=pd.NamedAgg(column='quantity', aggfunc='sum'),
        total_revenue=pd.NamedAgg(column='revenue', aggfunc='sum')
    ).reset_index()

    # Сортировка и расчет кумулятивного процента по количеству
    subcategory_stats = subcategory_stats.sort_values(by='total_quantity', ascending=False)
    subcategory_stats['cum_quantity_percent'] = subcategory_stats['total_quantity'].cumsum() / subcategory_stats[
        'total_quantity'].sum() * 100

    # Сортировка и расчет кумулятивного процента по выручке
    subcategory_stats = subcategory_stats.sort_values(by='total_revenue', ascending=False)
    subcategory_stats['cum_revenue_percent'] = subcategory_stats['total_revenue'].cumsum() / subcategory_stats[
        'total_revenue'].sum() * 100

    # Определяем группу ABC
    def define_abc_group(value):
        if value <= 70:
            return 'A'
        elif value <= 90:
            return 'B'
        else:
            return 'C'

    subcategory_stats['ABC_group_quantity'] = subcategory_stats['cum_quantity_percent'].apply(define_abc_group)
    subcategory_stats['ABC_group_revenue'] = subcategory_stats['cum_revenue_percent'].apply(define_abc_group)

    # Выводим результаты
    print(subcategory_stats)

# Вызов всех функций для анализа данных
def perform_analysis(df, df_orders):
    print("\nMost popular product group:")
    popular_product_group(df)

    print("\nSales by subcategories:")
    sales_by_subcategories(df)

    print("\nAverage check on specific date:")
    average_check(df_orders, '2022-01-13')

    print("\nPromo share in category 'Сыры':")
    promo_share_category(df, 'Сыры')  # Замените 'Сыры' на другую категорию, если хотите

    print("\nMargin by category:")
    calculate_margin_by_category(df)

    print("\nABC Analysis:")
    abc_analysis(df)


# Выполнение анализа
perform_analysis(df, df_orders)