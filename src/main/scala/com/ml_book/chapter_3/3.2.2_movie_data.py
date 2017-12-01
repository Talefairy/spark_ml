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
if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("First_App")
    sc = SparkContext(conf=conf)
    # 文件数据格式： 用户ID（user ID）、年龄（age）、性别（gender）、职业（occupation）和邮编（ZIP code）
    user_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.item")