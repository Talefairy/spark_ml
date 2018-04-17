# -*- coding: utf-8 -*-
# @Time    : 2017/11/28
# @Author  : fc.w

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.pyplot import hist
from pyspark import SparkConf, SparkContext


"""
第三章 Spark 上数据的获取、处理与准备

3.2.2 探索电影数据
"""

def convert_year(x):
    try:
        return int(x[-4:])
    except:
        # 若数据缺失年份则将其年份设为1900。在后续处理中会过滤掉这类数据
        return 1900

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("First_App")
    sc = SparkContext(conf=conf)
    # 文件数据格式： 用户ID（user ID）、年龄（age）、性别（gender）、职业（occupation）和邮编（ZIP code）
    movie_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.item")
    movie_fields = movie_data.map(lambda lines: lines.split("|"))
    num_movies = movie_data.count()
    # 解析发行年份
    years = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x))
    years_filtered = years.filter(lambda x: x != 1900)
    movie_ages = years_filtered.map(lambda yr: 1988 - yr).countByValue()
    values = movie_ages.values()
    bins = movie_ages.keys()
    bins = bins[: -10]
    hist(values, bins=bins, color='lightblue', normed=True)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16, 10)
    fig.show()




