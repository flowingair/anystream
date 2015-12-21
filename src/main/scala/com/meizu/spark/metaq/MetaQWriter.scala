package com.meizu.spark.metaq


//import com.amazonaws.services.sns.model.Topic
import com.taobao.metamorphosis.exception.MetaClientException
import org.apache.spark.{SparkConf, SparkContext, Logging}

//import com.taobao.gecko.core.util.StringUtils
import com.taobao.metamorphosis.client.MessageSessionFactory
import com.taobao.metamorphosis.client.MetaClientConfig
import com.taobao.metamorphosis.client.MetaMessageSessionFactory
//import com.taobao.metamorphosis.client.consumer.ConsumerConfig
import com.taobao.metamorphosis.client.producer.MessageProducer
//import com.taobao.metamorphosis.client.consumer.MessageListener
import com.taobao.metamorphosis.utils.ZkUtils
import com.taobao.metamorphosis.Message
//import com.taobao.metamorphosis.cluster.Partition

/**
 * @author ${user.name}
 */

case class MetaQWriter (
    private var zkConnect: String, /* Zookeeper 集群地址 */
    private var topic: String      /* 消息主题 */
) extends Logging {
    @transient private var sessionFactory: MessageSessionFactory = null
    @transient private var producer: MessageProducer = null

//    loadMetaQConfig()
//
//    private def loadMetaQConfig():Unit ={
//        if (topic == null) {
//            topic = System.getProperty("topic", "")
//        }
//
//        if (zkConnect == null) {
//            zkConnect = System.getProperty("zkConnect", "")
//        }
//
//        if (topic.trim.isEmpty) {
//            logError("Error invalid property topic",
//                new IllegalArgumentException("invalid property topic"))
//        }
//
//        if (zkConnect.trim.isEmpty) {
//            logError("Error invalid property zkConnect",
//                new IllegalArgumentException("invalid property zkConnect"))
//        }
//    }

    def init(): Unit = {
        val metaClientConfig = new MetaClientConfig()
        val zkConfig = new ZkUtils.ZKConfig()

        zkConfig.zkConnect = zkConnect
        metaClientConfig.setZkConfig(zkConfig)
        sessionFactory = new MetaMessageSessionFactory(metaClientConfig)

        producer = sessionFactory.createProducer()
        producer.publish(topic)
    }

    def close(): Unit = {
//        logInfo(s"Stopping MetaQ Consumer Stream with group: $group")
        if (producer != null){
            producer.shutdown()
            producer = null
        }
        if (sessionFactory != null){
            sessionFactory.shutdown()
            sessionFactory = null
        }
    }

    def getTopic = this.topic
    def sendMessage(msg : Message) = producer.sendMessage(msg)
}

//object MetaQWriter {
//  def main(args : Array[String]): Unit = {
//      val sparkConf = new SparkConf().setAppName("A demo of writing a RDD into MetaQ")
//      val sparkContext = new SparkContext(sparkConf)
//      val data = Range(0, Math.pow(10, 5).toInt, 1)
//      val metaqWriterRef = MetaQWriter("ORION_TEST")
//      val rdd = sparkContext.parallelize(data)
//      rdd.foreachPartition(part => {
//          val metaqWriter = metaqWriterRef.copy()
//          metaqWriter.init()
//          part.foreach(element => {
//              val msg =  new Message(metaqWriter.getTopic,element.toString.getBytes)
//              try {
//                  val sendResult = metaqWriter.sendMessage(msg)
//                  if (!sendResult.isSuccess) {
//                      metaqWriter.logError("Send message failed,error message:" + sendResult.getErrorMessage)
//                  }else {
//                      metaqWriter.logInfo("Send message successfully,sent to " + sendResult.getPartition)
//                  }
//              } catch {
//                  case e: MetaClientException => {
//                      metaqWriter.logError("Send message exception : " + e.getStackTraceString)
//                  }
//              }
//          })
//          metaqWriter.close()
//      })
//      sparkContext.stop()
//  }
//}
