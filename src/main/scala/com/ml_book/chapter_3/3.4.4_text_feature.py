# -*- coding: utf-8 -*-
# @Time    : 2017/11/28
# @Author  : fc.w

from pyspark import SparkConf, SparkContext


"""
第三章 Spark 上数据的获取、处理与准备

3.4.4 文本特征
"""

def extract_title(raw):
    import re
    # 该表达式找寻括号之间的非单词（数字）
    grps = re.search("\((\w+)\)", raw)
    if grps:
        # 只选取标题部分，并删除末尾的空白字符
        return raw[:grps.start()].strip()
    else:
        return raw

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("First_App")
    sc = SparkContext(conf=conf)
    # 文件数据格式： 用户ID（user ID）、年龄（age）、性别（gender）、职业（occupation）和邮编（ZIP code）
    movie_data = sc.textFile("../../../../resources/data/spark_ml/ml-100k/u.item")
    movie_fields = movie_data.map(lambda lines: lines.split("|"))
    num_movies = movie_data.count()

