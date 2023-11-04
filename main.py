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


def task_2():
    df1 = pd.read_csv('sample_supplier.csv', sep='	', engine='python')
    df2 = pd.read_csv('first_task.csv', sep='|', engine='python')

    df2 = df2.groupby(['part_number', 'manufacturer']).agg({'price': 'sum'}).reset_index()

    from sqlite3 import connect

    conn = connect(':memory:')

    df1.to_sql(name='sample_supplier', con=conn, index=False)
    df2.to_sql(name='first_task', con=conn, index=False)

    comparison = pd.read_sql(""" 
    SELECT 
    sample_supplier.part_number AS S_S_part_number,
    sample_supplier.manufacturer AS S_S_manufacturer,
    sample_supplier.price AS S_S_price,
    first_task.part_number AS F_T_part_number,
    first_task.manufacturer AS F_T_manufacturer,
    first_task.price AS F_T_price,
    
    CASE 
        WHEN sample_supplier.price < first_task.price THEN 'First-task-pricelist'
        WHEN sample_supplier.price > first_task.price THEN 'Sample-supplier-pricelist'
        ELSE 'Equal'
    END AS better_price_list
    FROM first_task sample_supplier
    JOIN sample_supplier first_task ON sample_supplier.part_number = first_task.part_number AND sample_supplier.manufacturer = first_task.manufacturer;
    """, conn)

    comparison.to_csv("task2_comparison.csv", sep=' ')


def task_3():
    csv.field_size_limit(100000000)
    multi = pd.read_fwf("/home/ydenys/Документи/ts/archive/PP0006_MULTI.csv",
                     widths=[26, 6, 20, 30, 3, 2, 24, 30, 20], encoding='latin-1')

    multi.columns = ['a', 'b', 'part_number','name','f','g','price','count in pack','discount group']
    multi = multi[['part_number','name', 'price', 'count in pack', 'discount group']]

    multi["price"] = multi["price"] / 100
    multi["count in pack"] = multi["count in pack"].str.replace("0", '')
    multi["count in pack"] = multi["count in pack"].str.replace(r'^.*?\s', '', regex=True) if len(multi["count in pack"]) > 2 else multi["count in pack"]
    multi["discount group"] = multi["discount group"].replace(r'^0+', '', regex=True)

    multi.to_csv("task_3.csv", sep='|', index=False)


concatenate_data()
task_2()
task_3()