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

sales_by_subcategories(df)
