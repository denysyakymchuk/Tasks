import csv

import pandas as pd


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
