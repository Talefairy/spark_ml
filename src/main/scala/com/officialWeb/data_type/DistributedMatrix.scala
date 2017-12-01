package com.officialWeb.data_type

import org.apache.spark.mllib.linalg.{Matrices, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * DistributedMatrix分布式矩阵 常用操作
  * Created by fc.w on 2017/11/27.
  */
object DistributedMatrix {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Distributed matrix Learning").setMaster("local[1]")
    val sc = new SparkContext(conf)
    // 建立稠密RDD[Vector]
    val rdd1: RDD[Vector] = sc.parallelize(Array(
      Array(1.0,7.0,0,0),
      Array(0,2.0,8.0,0),
      Array(5.0,0,3.0,9.0),
      Array(0,6.0,0,4.0)
    )).map(f => Vectors.dense(f))

    /* 创建行矩阵（RowMatrix） */
    val rowMatrix = new RowMatrix(rdd1)
    // rowMatirx调用columnSimliarities 的方式有两种
    // def columnSimilarities(threshold: Double): CoordinateMatrix
    // def columnSimilarities(): CoordinateMatrix

    // threshold = 1  返回是（）
    // threshold = 0.5
    // MatrixEntry(2,3,0.9738191526000504)
    // MatrixEntry(0,1,0.2607165735702116)
    // MatrixEntry(0,2,0.5586783719361678)
    // ()

    // 总共有6个，就是当没有输入参数的时候的个数
    println(rowMatrix.columnSimilarities(0.5).entries.collect().foreach(println))
    println("=================================exclude threshold==============================================")
    println(rowMatrix.columnSimilarities().entries.collect().foreach(x=>println(x)))
    println("行数：" + rowMatrix.numRows())//4
    println("列数：" + rowMatrix.numCols())//4

    println("========computeColumnSummaryStatistics()方法：触发action===========================")
    println("Array的size： " + rowMatrix.computeColumnSummaryStatistics().count)
    println("每列的最大值，组成的vector：" + rowMatrix.computeColumnSummaryStatistics().max)
    println("每列的最小值，组成的vector：" + rowMatrix.computeColumnSummaryStatistics().min)
    println("每列平均值：" + rowMatrix.computeColumnSummaryStatistics().mean)
    println("矩阵列的L1范数,也就是绝对值相加, ||x||1 = sum（abs(xi)）: " + rowMatrix.computeColumnSummaryStatistics().normL1)
    println("矩阵列的L2范数,也就是数的平方相加在取根号，||x||2 = sqrt(sum(xi.^2)): " + rowMatrix.computeColumnSummaryStatistics().normL1)
    println("每列非零元素的个数：" + rowMatrix.computeColumnSummaryStatistics().numNonzeros)
    println("每列的方差: " + rowMatrix.computeColumnSummaryStatistics().variance)

    println("----------------------------------协方差-----------------------------------------------")
    // 协方差：是衡量两个向量之间的总体误差(方差是协方差的特殊情况)
    // Cov(X,Y) = E((X-u)*((Y-v))),其中u是X的平均值，v是Y的平均值
    // 纯数学：就是比较两个向量的相关是  正 负 还是 相互独立
    println("协方差: " + rowMatrix.computeCovariance())

    println("----------------------------------格拉姆矩阵--------------------------------------------")
    // 定义：https://en.wikipedia.org/wiki/Gramian_matrix
    // 所有主子式大于等0 => 半正定矩阵
    // 格拉姆矩阵是半正定矩阵。
    println(rowMatrix.computeGramianMatrix())
    // 26.0  7.0   15.0  45.0
    // 7.0   89.0  16.0  24.0
    // 15.0  16.0  73.0  27.0
    // 45.0  24.0  27.0  97.0
    //其中26是 第1列组成的向量乘以第1列组成的向量，7是第1列组成的向量乘以第2列组成的向量.。

    println("-------------------------矩阵的主成分----------------------------")
    // 主成分分析本质就是矩阵转换。把原有的数据点变换到一个新的坐标。是一种降维的统计方法。
    // 后续会有详细介绍,输入的2是前2的主成分因子
    println("主成分分析: " + rowMatrix.computePrincipalComponents(2))

    println("-----------------SVD奇异值分界-----------------------")
    // 推荐博文：http://blog.chinaunix.net/uid-20761674-id-4040274.html
    // A = USV'，
    // U = A*A'
    // V = A'*A
    // S是一个特征向量
    // 源码返回的是：SingularValueDecomposition[RowMatrix, Matrix]
    println("SVD奇异值: " + rowMatrix.computeSVD(4,true))
    println("S的数值："+ rowMatrix.computeSVD(4,true).s)
    // rowMatrix.computeSVD(4,true).U是一个rowMatrix
    println("U的数值："+ rowMatrix.computeSVD(4,true).U.columnSimilarities().entries.collect().foreach(x => println(x)))
    println("V的数值："+ rowMatrix.computeSVD(4,true).V)

    println("=================矩阵相乘=======================")
    // def multiply(B: Matrix): RowMatrix={.................}
    // 返回的是RowMatrix类型
    val arr1 = Matrices.dense(4,1,Array(1.0,2.0,3.0,4.0))
    println(rowMatrix.multiply(arr1).columnSimilarities().entries.collect().foreach(println))


    // ===============IndexedRowMatrix的用法=======================
    // An IndexedRowMatrix is similar to a RowMatrix but with meaningful row indices.
    // It is backed by an RDD of indexed rows, so that each row is represented by its index (long-typed) and a local vector.
    // indexedRowMatrix可以从RDD创建而来，之后可以转变为RowMatrix
    // 错误:val indexedRowMatrix = new IndexedRowMatrix(rdd1)
    // 理由：rdd1是RDD[Vector]

    // Indexed的两种用法：
    // new IndexedRowMatrix(rows: RDD[IndexedRow])
    // new IndexedRowMatrix(rows: RDD[IndexedRow], nRows: Long, nCols: Int)
    // 查阅源码发现：case class IndexedRow(index: Long, vector: Vector)
    // index:我们可以自己选择数值,注意是Long类型
    // 函数实现
    // def keyOfIndex(key:Int):Long=key
    // def keyOfIndex(key:Double)=key.toLong
    // implicit函数实现
    implicit def DoubleToLong(key:Double)= key.toLong

    val rdd2 = sc.parallelize(Array(
      Array(1.0,7.0,0,0),
      Array(0,2.0,8.0,0),
      Array(5.0,0,3.0,9.0),
      Array(0,6.0,0,4.0)
    )).map(f => IndexedRow(f.takeRight(1)(0), Vectors.dense(f.drop(1))))
    println("rdd2的数值："+rdd2.collect.foreach(println))
    // f.take(1)(0),Vectors.dense(f.drop(1)))
    // f.take(1)得到是一个数组之后在选数组的第一个数字所以是f.take(1)(0)
    // 如果最右边的第一个是指标，前面n-1个的是参数
    // f.takeRight(1)(0),Vectors.dense(f.dropRight(1)))
    // 有了這些指标在训练网络的时候，非常方便，比如BP
    val IndexedRowMatrix = new IndexedRowMatrix(rdd2)
    // indexedRowMatrix转换为RowMatrix
    println(IndexedRowMatrix.toRowMatrix())
    // 当然还有一些基本的运算，可以参考rowMatrix部分
    println(IndexedRowMatrix.numCols())


    // ===============CoordinateMatrix的用法====================
    // new CoordinateMatrix(entries: RDD[MatrixEntry])
    // new CoordinateMatrix(entries: RDD[MatrixEntry], nRows: Long, nCols: Long)
    // 从返回的类型来看，coordinateMatrix有下面两种方式创建
    // val coordinateMatrix: CoordinateMatrix= rowMatrix.columnSimilarities(0.5)
    val coordinateMatrix: CoordinateMatrix= rowMatrix.columnSimilarities()
    // return：An n x n sparse upper-triangular matrix of cosine similarities between columns of this matrix.
    // CoordinateMatrix的用法

    coordinateMatrix.entries.collect.foreach(println)
    //也可以做其他矩阵的操作
    println(coordinateMatrix.numCols())
    println(coordinateMatrix.numRows())
    //也可以转换为IndexedRowMatrix
    println(coordinateMatrix.toIndexedRowMatrix())


    sc.stop()
  }

}
