package top.wordcout.jun.wc

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * first wordcount demo
  * word2018
  */
object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._

    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        9000
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream("localhost",port)

    val words: DataStream[String] = text.flatMap(w=>w.split(" "))
   // val wordAndOne: DataStream[(String, Int)] = words.map((_,1))
    val wordWithCount: DataStream[WordWithCount] = words.map(word=>WordWithCount(word,1))
   // val keyStream: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)
    val keyWordCount: KeyedStream[WordWithCount, Tuple] = wordWithCount.keyBy("word")
    val windowRes: WindowedStream[WordWithCount, Tuple, TimeWindow] = keyWordCount.timeWindow(Time.seconds(5),Time.seconds(1))
    val windowWordCount: DataStream[WordWithCount] = windowRes.sum("count")
    windowWordCount.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }

  case class WordWithCount(word:String,count:Long)
}
