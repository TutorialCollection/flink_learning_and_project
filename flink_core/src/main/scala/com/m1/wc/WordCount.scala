package com.m1.wc

import org.apache.flink.api.scala._

/**
 * @author moqi
 *         On 6/25/20 17:37
 */
object WordCount {

  val inputFilePath: String = "flink_core/src/main/resources/data.txt"

  def main(args: Array[String]): Unit = {

    val environment = ExecutionEnvironment.getExecutionEnvironment

    val inputDataSet = environment.readTextFile(inputFilePath)

    val resultDataSet = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    resultDataSet.print()
  }

}
