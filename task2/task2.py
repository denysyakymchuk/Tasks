import pandas as pd


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
