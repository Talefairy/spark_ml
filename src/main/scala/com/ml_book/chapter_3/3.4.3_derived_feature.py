# -*- coding: utf-8 -*-
# @Time    : 2017/12/04
# @Author  : fc.w

import numpy as np
from pyspark import SparkConf, SparkContext


"""
第三章 Spark 上数据的获取、处理与准备

3.2.3 派生特征
    从现有的一个或多个变量派生出新的特征常常是有帮助的。理想情况下，派生出的特征能比原始属性带来更多信息。
    原始数据派生特征的例子包括计算平均值、中位值、方差、和、差、最大值或最小值以及计数。

将时间戳转为类别特征
    以对评级时间的转换为例，说明如何将数值数据装换为类别特征。该时间的格式为Unix的时间戳。
    我们可以用Python的datetime模块从中提取出日期、时间以及点钟（hour）信息。
"""


def extract_datetime(ts):
    """
    评级时间戳提取为datetime的格式
    """
    import datetime
    return datetime.datetime.fromtimestamp(ts)


def assign_tod(hr):
    times_of_day = {
        'morning' : range(7, 12),
        'lunch' : range(12, 14),
        'afternoon' : range(14, 18),
        'evening' : range(18, 23),
        'night' : range(23, 7)
    }


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("First_App")
    sc = SparkContext(conf=conf)
    rating_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.item")

    timestamps = rating_data.map(lambda fields: int(fields[3]))
    hour_of_day = timestamps.map(lambda ts: extract_datetime(ts).hour)
    print hour_of_day.take(5)

