# -*- coding: utf-8 -*-
# @Time    : 2017/12/04
# @Author  : fc.w

import numpy as np
from pyspark import SparkConf, SparkContext

"""
第三章 Spark 上数据的获取、处理与准备

3.3 数据转换与处理
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
    movie_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.item")
    # 获取电影数
    num_movies = movie_data.count()
    movie_fields = movie_data.map(lambda lines: lines.split("|"))
    years_pre_processed = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x)).collect()
    years_pre_processed_array = np.array(years_pre_processed)
    # 计算发行年份的平均数和中位数
    mean_year = np.mean(years_pre_processed_array[years_pre_processed_array != 1900])
    median_year = np.median(years_pre_processed_array[years_pre_processed_array != 1900])
    # 用numpy的函数来找出year_pre_processed_array中的非规整数据点的序号
    index_bad_data = np.where(years_pre_processed_array == 1900)[0][0]
    # 该序号来将中位数作为非规整数据的发行年份
    years_pre_processed_array[index_bad_data] = median_year

    print "Mean year of release: %d" % mean_year
    print "Median year of release: %d" % median_year
    print "Index of '1900' after assigning median: %s" % np.where(years_pre_processed_array == 1900)[0]
