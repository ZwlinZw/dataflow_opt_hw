import java.util.{Properties, UUID}

import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import com.bingocloud.{ClientConfiguration, Protocol}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.nlpcn.commons.lang.util.IOUtil

import scala.collection.mutable

object Demo {
  //s3参数
  val accessKey = "DE6EEA2A384A7A79314D"
  val secretKey = "WzhDMEIyMjlDRURFOUYwNDRBQ0ZGMEJGQTczMzkyN0VDQzEwNkVFRkRd"
  val endpoint = "scuts3.depts.bingosoft.net:29999"
  val bucket = "linzhiwei"
  //要读取的文件
  val key = "daas.txt"
  //上传文件的路径前缀
  val keyPrefix = "store1/"
  //上传数据间隔 单位毫秒
  val period = 5000
  //kafka参数
  val topic = "linzhiwei"
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"
  /**
   * 从s3中读取文件内容
   *
   * @return s3的文件内容
   */
  def readFile(): String = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    val amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)
    val s3Object = amazonS3.getObject(bucket, key)
    IOUtil.getContent(s3Object.getObjectContent, "UTF-8")
  }

  /**
   * 把数据写入到kafka中
   *
   * @param s3Content 要写入的内容
   */
  def produceToKafka(s3Content: String): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = s3Content.split("\n")
    for (s <- dataArr) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String](topic, null, s)
        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }
  def getCity(string: String): String={
    var result=new Array[String](5)
    result=string.split(",")
    var mid= result(4)
    return result(4).split(":")(1)
  }
  def consumerFromkafka():Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)
    inputKafkaStream.map(x=>(x,1)).timeWindowAll(Time.seconds(5))
      .process(new ProcessAllWindowFunction[(String, Int), String, TimeWindow] {
        override def process(context: Context, elements: Iterable[(String,Int)], out: Collector[String]): Unit = {

          val wordCountMap = mutable.Map[String, Int]()
          //定义两个数组，1 城市名 2 相对的数目
          var city: Array[String] = new Array[String](200)
          var cityNum = 0
          var touristNum: Array[Int] = new Array[Int](200)

          elements.foreach(kv => {

            //如果城市名数组没有当前的城市名，将当前城市名加入数组，并将城市数加一，并将对应的游客数设为一
            if (!city.contains(getCity(kv._1))) {
              city(cityNum) = getCity(kv._1)
              touristNum(cityNum) = 1
              cityNum += 1
            }
          })
          val result = Array.ofDim[String](cityNum, 300)
          elements.foreach(kv=>{
            //对每一条数据根据城市名找到city数组对应的下标，将其存入二维数组中
            var i=0
            for(i<-0 to cityNum-1) {
              //找到该记录所对应的城市，根据下标i找到touristNum数组的下标，将其存入result中
              if (city(i) == getCity(kv._1)) {
                  result(i)(touristNum(i)-1)=kv._1
                  touristNum(i)+=1
              }
            }
        })

          for(x<-0 to cityNum-1){
            for(y<-0 to 299)
              { if(result(x)(y)!=null)
                out.collect(result(x)(y))
              }
          }
        }
      }).writeUsingOutputFormat(new S3Writer(accessKey, secretKey, endpoint, bucket, keyPrefix, period))
    //将数据进行分类
//    inputKafkaStream.map(x=>JSON.parseFull(x)).map(x=>x match{
//      case Some(m: Map[String,Any])=>m("destination")match {
//        case s:String =>s}
//      }).map((_,1)).keyBy(0).sum(1).map(x=>x.toString()).writeUsingOutputFormat(new S3Writer(accessKey, secretKey, endpoint, bucket, keyPrefix, period))
    env.execute()
  }
  def main(args: Array[String]): Unit = {
    //读取S3文件内容
//    val s3Content = readFile()
//    //写入kafka
//    produceToKafka(s3Content)
    //从kafka读取内容
    consumerFromkafka()
  }
}
