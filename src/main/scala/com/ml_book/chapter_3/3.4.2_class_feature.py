# -*- coding: utf-8 -*-
# @Time    : 2017/12/04
# @Author  : fc.w

import numpy as np
from pyspark import SparkConf, SparkContext

"""
第三章 Spark 上数据的获取、处理与准备

3.4.2 类别特征
"""
if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("First_App")
    sc = SparkContext(conf=conf)
    # 文件数据格式： 用户ID（user ID）、年龄（age）、性别（gender）、职业（occupation）和邮编（ZIP code）
    user_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.user")
    user_fields = user_data.map(lambda line: line.split("|"))
    all_occupations = user_fields.map(lambda fields: fields[3]). distinct().collect()
    all_occupations.sort()

    # 然后可以依次对各可能的职业分配序号（注意，为与Python、Scala以及Java中数组编序相同，这里也从0开始编号）：
    idx = 0
    all_occupations_dict = {}
    for o in all_occupations:
        all_occupations_dict[o] = idx
        idx += 1

    # 看一下“k之1”编码会对新的例子分配什么值
    print "Encoding of 'doctor': %d" % all_occupations_dict['doctor']
    print "Encoding of 'programmer': %d" % all_occupations_dict['programmer']

    # 首先需创建一个长度和可能的职业数目相同的numpy数组，其各元素值为0。
    # 这可通过numpy的zeros函数实现。
    # 之后将提取单词programmer的序号，并将数组中对应该序号的那个元素值赋为1：
    k = len(all_occupations_dict)
    binary_x = np.zeros(k)
    k_programmer = all_occupations_dict['programmer']
    binary_x[k_programmer] = 1
    print "Binary feature vector: %s" % binary_x
    print "Length of binary vector: %d" % k
