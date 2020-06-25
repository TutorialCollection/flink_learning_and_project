package com.hp.wc

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    //val hostname: String = parameterTool.get("host")
    //val port: Int = parameterTool.getInt("port")

    // 使用 nc -lk 9999 不断输入数据流
    val inputDataStream: DataStream[String] = environment.socketTextStream("localhost", 9999)

    val result: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" ")).startNewChain()
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    result.print()

    environment.execute("stream wc job")
  }
}
