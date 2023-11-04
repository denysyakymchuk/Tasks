import csv
import pandas as pd


def count_brands(data, column: str):
    """
    :param data: DataFrame
    :param column: the name of the column in DataFrame in which to count
    """
    brand_counts = data.groupby(column).size().reset_index(name='counts')

    for index, row in brand_counts.iterrows():
        with open("analysis_manufacturers.txt", 'a+') as file:
            file.write(f"{row[column]} — {row['counts']} rows\n")


def concatenate_data():
    data = pd.read_csv('data.csv', sep='	', engine='python')
    prices = pd.read_csv('prices.csv', sep='	', engine='python', decimal=',')
    qua = pd.read_csv('quantity.csv', sep='	', engine='python')

    qua.columns = ['part_number', 'quantity']

    actual_data = pd.merge(data, prices, how="left", on='part_number')
    actual_data = pd.merge(actual_data, qua, how="left", on='part_number')

    actual_data = actual_data.dropna(axis=0)
    actual_data = actual_data[actual_data['price'] > 0]

    actual_data.to_csv("first_task.csv", index=False, sep='|')

    count_brands(actual_data, 'manufacturer')

