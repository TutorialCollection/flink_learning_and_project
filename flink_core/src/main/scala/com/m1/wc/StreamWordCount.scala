package com.m1.wc

import org.apache.flink.streaming.api.scala._

/**
 * @author moqi
 *         On 6/25/20 17:41
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDataStream = environment.socketTextStream("localhost", 9999)

    val result = inputDataStream.flatMap(_.split(" ")).startNewChain().map((_, 1)).keyBy(_._1).sum(1)

    result.print()

    environment.execute("stream word count job")

  }

}
