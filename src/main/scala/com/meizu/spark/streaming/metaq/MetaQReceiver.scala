package com.meizu.spark.streaming.metaq

//import java.io.IOException
//import java.util.Properties
import java.nio.ByteBuffer
import java.util.concurrent.Executor

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

//import com.taobao.gecko.core.util.StringUtils
import com.taobao.metamorphosis.client.MessageSessionFactory
import com.taobao.metamorphosis.client.MetaClientConfig
import com.taobao.metamorphosis.client.MetaMessageSessionFactory
import com.taobao.metamorphosis.client.consumer.ConsumerConfig
import com.taobao.metamorphosis.client.consumer.MessageConsumer
import com.taobao.metamorphosis.client.consumer.MessageListener
import com.taobao.metamorphosis.client.consumer.LoadBalanceStrategy
import com.taobao.metamorphosis.utils.ZkUtils

//import com.meizu.jmetric.common.ResourceUtils
//import com.meizu.jmetric.common.SimpleConfigCenterClient
import com.taobao.metamorphosis.Message
//import com.taobao.metamorphosis.cluster.Partition


/**
 * Created by kuangdonglin on 2015/4/15.
 */

// case class MetaQMessage(topic:String, data: Array[Byte], attribute: Option[String])

class MetaQReceiver (
    private val zkConnect: String, /* MetaQ ZK集群地址 */
    private val topic: String,     /* 消息主题 */
    private val group: String,     /* 消息分组 */
//    private val runners: Int
    private val metaqParams : Map[String, String]
                               )extends Receiver[Message](StorageLevel.MEMORY_AND_DISK_SER) with Logging {

    @transient private var sessionFactory: MessageSessionFactory = null
    @transient private var consumer: MessageConsumer = null

    override def onStart(): Unit ={
         logInfo(s"Starting MetaQ Consumer Stream with group: $group")
         initMetaQ()
         consumer.subscribe(topic, 1024 * 1024, new MessageListener() {
             def recieveMessages (message: Message): Unit = {
                 try {
                     store(message)
                 } catch {
                     case iex: InterruptedException => throw iex
                     case _ : Throwable => throw new RuntimeException
                 }
             }

             def getExecutor: Executor = {
                 null
             }
         })
         consumer.completeSubscribe()
    }

    override def onStop(): Unit = {
        logInfo(s"Stopping MetaQ Consumer Stream with group: $group")
        if (consumer != null){
            consumer.shutdown()
            consumer = null
        }
        if (sessionFactory != null){
            sessionFactory.shutdown()
            sessionFactory = null
        }
    }

    private def initMetaQ(): Unit = {
        val metaClientConfig = new MetaClientConfig()
        val consumerConfig = new ConsumerConfig(group)
        val zkConfig = new ZkUtils.ZKConfig()

        zkConfig.zkConnect = zkConnect
        metaClientConfig.setZkConfig(zkConfig)
        sessionFactory = new MetaMessageSessionFactory(metaClientConfig)

        consumerConfig.setFetchRunnerCount(1)
        metaqParams.foreach( pair => {
            val (key, value) = pair
            key match {
                case "offset" => consumerConfig.setOffset(value.toLong)
                case "partition" => consumerConfig.setPartition(value)
                case "fetchRunnerCount" => consumerConfig.setFetchRunnerCount(value.toInt)
                case "fetchTimeoutInMills" => consumerConfig.setFetchTimeoutInMills(value.toLong)
                case "maxFetchRetries" => consumerConfig.setMaxFetchRetries(value.toInt)
                case "maxIncreaseFetchDataRetries" => consumerConfig.setMaxIncreaseFetchDataRetries(value.toInt)
                case "maxDelayFetchTimeInMills" => consumerConfig.setMaxDelayFetchTimeInMills(value.toLong)
                case "consumeFromMaxOffset" => if (value.toBoolean) consumerConfig.setConsumeFromMaxOffset(true)
                case "commitOffsetPeriodInMills" => consumerConfig.setCommitOffsetPeriodInMills(value.toLong)
                case "loadBalanceStrategyType" => consumerConfig.setLoadBalanceStrategyType(LoadBalanceStrategy.Type.valueOf(value))
                case _ =>
            }
        })

        consumer = sessionFactory.createConsumer(consumerConfig)
    }
}




