#!/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ : stray_camel
# __date__: 2020/05/07 18:54:44

import logging
import datetime

# 打印时间的装饰器
def logging_time(func):
    def wrapper(*args, **kwargs):
        start = datetime.datetime.now()
        print("this function <", func.__name__, ">is running")
        res = func(*args, **kwargs)
        print("this function <", func.__name__,
              "> takes time：", datetime.datetime.now()-start)
        return res
    return wrapper
