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
from PIL import Image
from pytesseract import image_to_string

root_path = os.path.abspath("..")
modules_path = "/modules3"
lib_path = "/lib3"

sys.path.append(root_path + modules_path)
sys.path.append(root_path + lib_path)

pd.set_option("display.max_columns", 1000)  # show all columns
pd.set_option("display.max_colwidth", 1000)  # show each column entirely
pd.set_option("display.max_rows", 300)  # show all rows


def func():
    """
    Description:
    
    """
    pass


if __name__ == '__main__':
    """main code here"""
    print(image_to_string(Image.open('sweetiq_keys_2.png')))
    # print(image_to_string(Image.open('test-english.jpg'), lang='eng'))