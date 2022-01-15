#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
Subject:
Date:
Version:
Description:
"""

import sys
import os
import datetime as dt
import time as t
import pandas as pd

root_path = os.path.abspath("..")
modules_path = "/modules3"
lib_path = "/lib3"

sys.path.append(root_path + modules_path)
sys.path.append(root_path + lib_path)

# from conns import *
# from common import *
# from CommCls import *
import statistics as sta

pd.set_option("display.max_columns", 1000)  # show all columns
pd.set_option("display.max_colwidth", 1000)  # show each column entirely
pd.set_option("display.max_rows", 300)  # show all rows

# list1 = [113,257,280,21,165]
# list2 = [55,139,277,34,141]
# list3 = [85,204,116,44,264]

list1, list2, list3 = [189,257,117,213,532], [413,15,189,415,469], [109,265,421,181,53]

def func(lst):
    """
    Description:
    
    """
    m = max(lst)
    k = len(lst)
    return m + (m - k) / k


if __name__ == '__main__':
    """main code here"""

    l = [list1, list2, list3, list1 + list2, list2 + list3, list1 + list3, list1 + list2 + list3]

    r = list(map(lambda x: func(x), l))
    print(r)
    print({"avg": sta.mean(r), "median": sta.median(r)})