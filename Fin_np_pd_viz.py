import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Чтение файлов и формирование датафреймов
df_orders = pd.read_excel('/home/zega_r/Desktop/Education/Simulative/Final_project /data/orders.xlsx')
df_products = pd.read_excel('/home/zega_r/Desktop/Education/Simulative/Final_project /data/products.xlsx')

# Функция определения самой ходовой группы товара и отображение на графике
def popular_product_group(df_orders, df_products):
    # Объединение датафреймов и группировка результата
    df = pd.merge(df_products, df_orders, on='product_id')
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

