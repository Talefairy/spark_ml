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

3.2.1 探索用户数据
"""
if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("First_App")
    sc = SparkContext(conf=conf)
    # 文件数据格式： 用户ID（user ID）、年龄（age）、性别（gender）、职业（occupation）和邮编（ZIP code）
    user_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.user")
    user_fields = user_data.map(lambda line: line.split("|"))
    # 获取用户数
    num_users = user_fields.map(lambda fields: fields[0]).count()
    # 获取去重后的性别数
    num_genders = user_fields.map(lambda fields: fields[2]).distinct().count()
    # 获取职业数
    num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()
    # 邮编数
    num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()
    print "Users: %d, genders: %d, occupations: %d, ZIP codes: %d" % (num_users, num_genders, num_occupations, num_zipcodes)

    # 年龄数量条形图
    ages = user_fields.map(lambda x: int(x[1])).collect()
    # ages数组、直方图的bins数目（即区间数，这里为20）normed=True参数来正则化直方图
    hist(ages, bins=20, color='lightblue', normed=True)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16, 10)
    fig.show()

    # 职业数量条形图
    count_by_accupation2 = user_fields.map(lambda fields: (fields[3], 1)).reduceByKey(lambda x, y: x + y).collect()
    x_axis1 = np.array([c[0] for c in count_by_accupation2])
    y_axis1 = np.array([c[1] for c in count_by_accupation2])
    # 数量升序
    x_axis = x_axis1[np.argsort(x_axis1)]
    y_axis = y_axis1[np.argsort(y_axis1)]

    pos = np.arange(len(x_axis))
    width = 1.0
    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))
    ax.set_xticklabels(x_axis)
    plt.bar(pos, y_axis, width, color='lightblue')
    plt.xticks(rotation=30)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16, 10)
    fig.show()

