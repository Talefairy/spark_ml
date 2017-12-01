package com.officialWeb.data_type

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * 1. Data Types - RDD-based API
  * Created by fc.w on 2017/11/27.
  */
object DataType {

  def main(args: Array[String]): Unit = {
    // 1. 本地向量
    localVector()
  }

  /**
    * 1. 本地向量
    */
  def localVector(): Unit = {
    // 创建稠密向量
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    print(dv)
    // 创建稀疏向量(1.0, 0.0, 3.0) 通过指定与非零项相关的索引和值。
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // 创建稀疏向量(1.0, 0.0, 3.0) 通过指定非零项
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

  }

  /**
    * 2. 标记点
    */
  def labeledPoint(): Unit = {
    // 用正标签和一个稠密向量，创建一个标记点
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    // 用负标签和一个稀疏向量，创建一个标记点
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
  }

  /**
    * 3. 本地矩阵
    */
  def localMatrix(): Unit = {
    // 创建3行2列稠密矩阵((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    // 创建3行2列稀疏矩阵((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
  }

//  /**
//    * 4. 分布式矩阵 之 行矩阵
//    */
//  def rowMatrix(): Unit = {
//    // an RDD of local vectors
//    // val rows: RDD[Vector] = ...
//    // 通过RDD[Vector]创建行矩阵
//    val mat: RowMatrix = new RowMatrix(RDD[Vector])
//
//    // 获取矩阵行列大小
//    val m = mat.numRows()
//    val n = mat.numCols()
//
//    // QR decomposition
//    val qrResult = mat.tallSkinnyQR(true)
//  }

//  /**
//    * 4. 分布式矩阵 之 下标行矩阵
//    */
//  def indexedRowMatrix(): Unit = {
//    // an RDD of indexed rows
//    // val rows: RDD[IndexedRow] = ...
//    val mat: IndexedRowMatrix = new IndexedRowMatrix(RDD[IndexedRow])
//    // 获取矩阵行列大小
//    val m = mat.numRows()
//    val n = mat.numCols()
//
//    // Drop its row indices.
//    val rowMat: RowMatrix = mat.toRowMatrix()
//  }
//
//  /**
//    * 4. 分布式矩阵 之 坐标矩阵
//    *  Each entry is a tuple of (i: Long, j: Long, value: Double)
//    */
//  def coordinateMatrix(): Unit = {
//    // an RDD of matrix entries
//    // val entries: RDD[MatrixEntry] = ...
//    // Create a CoordinateMatrix from an RDD[MatrixEntry].
//    val mat: CoordinateMatrix = new CoordinateMatrix(RDD[MatrixEntry])
//    // 获取矩阵行列大小
//    val m = mat.numRows()
//    val n = mat.numCols()
//
//    // Convert it to an IndexRowMatrix whose rows are sparse vectors.
//    val indexedRowMatrix = mat.toIndexedRowMatrix()
//  }
//
//  /**
//    * 5. 分布式矩阵 之 块矩阵
//    *
//    */
//  def blockMatrix(): Unit = {
//    // an RDD of (i, j, v) matrix entries
////    val entries: RDD[MatrixEntry] = ...
//// Create a CoordinateMatrix from an RDD[MatrixEntry].
//
//    val coordMat: CoordinateMatrix = new CoordinateMatrix(RDD[MatrixEntry])
//    // Transform the CoordinateMatrix to a BlockMatrix
//    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()
//
//    // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
//    // Nothing happens if it is valid.
//    matA.validate()
//
//    // Calculate A^T A.
//    val ata = matA.transpose.multiply(matA)
//  }

}

