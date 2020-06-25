package com.hp.wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputFilePath: String = "flink_learning/src/main/resources/data.txt"
    val inputDataSet: DataSet[String] = environment.readTextFile(inputFilePath)

    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    resultDataSet.print()
  }
}
