# -*- coding: utf-8 -*-
# @Time    : 2017/12/04
# @Author  : fc.w

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import hist
from pyspark import SparkConf, SparkContext


"""
第三章 Spark 上数据的获取、处理与准备

3.2.3 探索评级数据
"""

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("First_App")
    sc = SparkContext(conf=conf)
    rating_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.data")
    num_ratings = rating_data.count()

    # 文件数据格式： 用户ID（user ID）、年龄（age）、性别（gender）、职业（occupation）和邮编（ZIP code）
    user_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.user")
    user_fields = user_data.map(lambda line: line.split("|"))
    # 获取用户数
    num_users = user_fields.map(lambda fields: fields[0]).count()

    # 文件数据格式： 用户ID（user ID）、年龄（age）、性别（gender）、职业（occupation）和邮编（ZIP code）
    movie_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.item")
    movie_fields = movie_data.map(lambda lines: lines.split("|"))
    # 获取电影数
    num_movies = movie_data.count()

    rating_data = rating_data.map(lambda line: line.split("\t"))
    ratings = rating_data.map(lambda fields: int(fields[2]))
    max_rating = ratings.reduce(lambda x, y: max(x, y))
    min_rating = ratings.reduce(lambda x, y: min(x, y))
    mean_rating = ratings.reduce(lambda x, y: x + y) / num_ratings
    median_rating = np.median(ratings.collect())
    ratings_per_user = num_ratings / num_users
    ratings_per_movie = num_ratings / num_movies
    print "Min rating: %d" % min_rating
    print "Max rating: %d" % max_rating
    print "Average rating: %2.2f" % mean_rating
    print "Median rating: %d" % median_rating
    print "Average # of ratings per user: %2.2f" % ratings_per_user
    print "Average # of ratings per movie: %2.2f" % ratings_per_movie

    # 该函数包含一个数值变量用于做类似的统计
    print ratings.stats()

    count_dy_rating = ratings.countByValue()
    x_axis = np.array(count_dy_rating.keys())
    y_axis = np.array([float(c) for c in count_dy_rating.values()])
    # 这里对y轴正则化，使它表示百分比
    y_axis_normed = y_axis / y_axis.sum()
    pos = np.arange(len(x_axis))
    width = 1.0

    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))
    ax.set_xticklabels(x_axis)

    plt.bar(pos, y_axis_normed, width, color='lightblue')
    plt.xticks(rotation=30)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16, 10)
    fig.show()

    # 以用户ID为主键、评级为值的键值对
    user_ratings_grouped = rating_data.map(lambda fields: (int(fields[0]), int(fields[2]))).groupByKey()
    user_ratings_byuser = user_ratings_grouped.map(lambda (k, v): (k, len(v)))
    user_ratings_byuser_local = user_ratings_byuser.map(lambda (k, v): v).collect()
    # 来绘制各用户评级分布的直方图
    hist(user_ratings_byuser_local, bins=200, color='lightblue', normed=True)
    fig1 = matplotlib.pyplot.gcf()
    fig1.set_size_inches(16, 10)
    fig1.show()

