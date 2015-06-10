package com.meizu.anystream

import sun.misc.{Signal, SignalHandler}
import java.sql.{SQLException, Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date
import com.google.protobuf.InvalidProtocolBufferException
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.io.Source
import scala.collection.mutable
import org.rogach.scallop.{ScallopOption, ScallopConf}

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.taobao.metamorphosis.exception.MetaClientException
import com.taobao.metamorphosis.Message

import com.meizu.spark.streaming.metaq.MetaQReceiver
import com.meizu.spark.metaq.MetaQWriter
import org.elasticsearch.node.NodeBuilder._


class ArgsOptsConf (arguments: Seq[String]) extends ScallopConf(arguments) {

    banner("""Usage: anystream [OPTION]...
             |anystream is a framework based on spark streaming for data streaming processing in SQL/HQL.
             |Options:
             |""".stripMargin)
//    val properties: Map[String, String] = propsLong[String](name = "hiveconf", keyName = "property", descr = "Use value for given property")
    val confPath: ScallopOption[String] = opt[String](required = true, name = "conf", argName = "properties-file",
        noshort = true, descr = "configuration file")
    val transHqlPath : ScallopOption[String] = opt[String](required = false, name = "transform",
        argName = "transform-HQL-file", noshort = true, descr = "transform hql script")
    val lowLatencyHqlPath : ScallopOption[String] = opt[String](required = false, name = "ll_action",
        argName = "action-HQL-file", noshort = true, descr = "hql script for stream with low latency")
    val highLatencyHqlPath : ScallopOption[String] = opt[String](required = false, name = "hl_action",
        argName = "action-HQL-file", noshort = true, descr = "hql script for stream with high latency")
//    val hqlPath : ScallopOption[String] = trailArg[String](required = true, name = "hql-script", descr = "hive sql file to execute")
}

case class Load (
    interface:String,
    magic:java.lang.Integer,
    partition:String,
    data:Array[Byte],
    ext_domain:Map[String,String],
    config_id:java.lang.Integer,
    send_timestamp:java.lang.Long
)

/**
 * @author ${user.name}
 */
object ETL extends Logging {
    @transient private var instance: HiveContext = null
    private  val metaqMatcher = (
            """(?s)^\s*[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+""" +
            """[dD][iI][rR][eE][cC][tT][oO][rR][yY]\s+""" +
            """'([mM][eE][tT][aA][qQ]:.*?)'\s+(.*)""").r
    private val jdbcMatcher = (
            """(?s)^\s*[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+""" +
            """[dD][iI][rR][eE][cC][tT][oO][rR][yY]\s+""" +
            """'([jJ][dD][bB][cC]:.*?)'\s*""" +
            """(?:[sS][qQ][lL]\s+[oO][nN]\s+([eE][nN][tT][eE][rR]|[eE][xX][iI][tT])\s*'((?:[^']|\\')*)'\s*)?""" +
            """(?:[sS][qQ][lL]\s+[oO][nN]\s+([eE][nN][tT][eE][rR]|[eE][xX][iI][tT])\s*'((?:[^']|\\')*)'\s*)?""" +
            """((?:[sS][eE][lL][eE][cC][tT]|[wW][iI][tT][hH]).*)""").r
    private  val esMatcher = (
            """(?s)^\s*[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+""" +
            """[dD][iI][rR][eE][cC][tT][oO][rR][yY]\s+""" +
            """'([eE][sS]:.*?)'\s+(.*)""").r

    private val metaqURLExtractor = """[mM][eE][tT][aA][qQ]://([^/]*)/([^/\?]*)(?:\?(.*))?""".r
    private val jdbcURLExtractor = """(.*)/([^/\?]*)(\?.*)?""".r
    private val esURLExtractor = """[eE][sS]://([^/\?]*)""".r
    private val dayPartitionMatcher = """([\d]*)?[d|D]""".r
    private val hourPartitionMatcher = """([\d]*)?[h|H]""".r
    private val minutePartitionMatcher = """([\d]*)?[m|M]""".r


    // Instantiate HiveContext on demand
    def getInstance(sparkContext: SparkContext): HiveContext = synchronized {
        if (instance == null) {
            instance = new HiveContext(sparkContext)
            instance.udf.register("partitioner", (partition: String, timestamp: Long) => {
                partition match {
                    case dayPartitionMatcher(dInterval) =>
                        val interval = if (dInterval != null && !dInterval.isEmpty) {
                            (if (dInterval.toInt == 0) 1 else dInterval.toInt) * 24 * 3600 * 1000L
                        } else {
                            24 * 3600 * 1000L
                        }
                        val dt = new Date(timestamp - timestamp % interval)
                        val dtFormatter = new SimpleDateFormat("yyyyMMdd")
                        new java.lang.Long(dtFormatter.format(dt).toLong)
                    case hourPartitionMatcher(hInterval) =>
                        val interval = if (hInterval != null && !hInterval.isEmpty) {
                            (if (hInterval.toInt == 0) 1 else hInterval.toInt) * 3600 * 1000L
                        } else {
                            3600 * 1000L
                        }
                        val dt = new Date(timestamp - timestamp % interval)
                        val dtFormatter = new SimpleDateFormat("yyyyMMddHH")
                        new java.lang.Long(dtFormatter.format(dt).toLong)
                    case minutePartitionMatcher(mInterval) =>
                        val interval = if (mInterval != null && !mInterval.isEmpty) {
                            (if (mInterval.toInt == 0) 1 else mInterval.toInt) * 60 * 1000L
                        } else {
                            60 * 1000L
                        }
                        val dt = new Date(timestamp - timestamp % interval)
                        val dtFormatter = new SimpleDateFormat("yyyyMMddHHmm")
                        new java.lang.Long(dtFormatter.format(dt).toLong)
                    case _ => null
                }
            })
        }
        instance
    }

    def getProperty(key: String, default: Option[String]): String = {
        if (key == null) {
            throw new IllegalArgumentException("invalid property key")
        }
        val value = System.getProperty(key)
        if (value == null) {
            default match {
                case Some(prop) => prop
                case None => throw new IllegalArgumentException("invalid property " + key)
            }
        } else {
            if (value.trim.isEmpty){
                throw new IllegalArgumentException("invalid property " + key)
            } else {
                value.trim
            }
        }
    }

    def parseHql(path: String): List[(String, String)] = {
        val lines = Source.fromFile(path).getLines().toList
        val linesWithoutLineComment = lines.filter(line => ! (line.trim.startsWith("--") || line.trim.isEmpty))
                .map(_ + "\n").fold("")(_ + _)
        val hqlSeparator = 0x7F.toChar
        val hqlStr = for ( i <- 0 until linesWithoutLineComment.size) yield {
            if  (( i == 0 || linesWithoutLineComment.charAt(i - 1) != '\\' ) && linesWithoutLineComment.charAt(i) == ';')
                hqlSeparator
            else
                linesWithoutLineComment.charAt(i)
        }

        val hqlList = String.copyValueOf(hqlStr.toArray)
                .split(hqlSeparator)
                .map(ele =>
                     ele.split("\n")
                        .filter(line => !(line.trim.startsWith("--") || line.trim.isEmpty))
                        .map(_ + "\n").fold("")(_ + _)
                )
                .filter(!_.trim.isEmpty)
                .toList

        for ( hql <- hqlList) yield {
            val tagHql = hql.split(":=")
            if (tagHql.length == 1) {
                ("_", tagHql(0).trim)
            } else {
                (tagHql(0).trim, tagHql(1).trim)
            }
        }
    }

    def writeMetaQ(hqlContext : HiveContext, metaqURL : String, hql : String) : DataFrame = {
        val df = hqlContext.sql(hql)
        val (zkCluster,  topic) = metaqURL match {
            case metaqURLExtractor(zkClusterAddr, topicName, parameters) => (zkClusterAddr, topicName)
        }
        val metaqWriterRef = MetaQWriter(zkCluster, topic)
        df.rdd.foreachPartition(part => {
            val metaqWriter = metaqWriterRef.copy()
            metaqWriter.init()
            part.foreach(element => {
                val data = element match {
                    case Row(col1: Array[Byte]) => col1
                    case _ => null
                }
                if (data != null) {
                    val msg = new Message(metaqWriter.getTopic, data) // element.toString().getBytes
                    try {
                        val sendResult = metaqWriter.sendMessage(msg)
                        if (!sendResult.isSuccess) {
                            logError("Send message failed,error message:" + sendResult.getErrorMessage)
                        } else {
                            logInfo("Send message successfully,sent to " + sendResult.getPartition)
                        }
                    } catch {
                        case e: MetaClientException =>
                            logError("Send message exception : " + e.getStackTraceString)
                    }
                }
            })
            metaqWriter.close()
        })
        hqlContext.emptyDataFrame
    }

    def writeJDBC(hqlContext : HiveContext,
                jdbcPath : String,
                trigger1 : String,
                trigger1Sql : String,
                trigger2 : String,
                trigger2Sql : String,
                hql : String): DataFrame = {
        val df = hqlContext.sql(hql)
        val (resDescriptor, tableName, parameters) = jdbcPath match {
            case jdbcURLExtractor(res, table, paras) => (res, table, paras)
        }
        val jdbcURL = if (parameters != null) resDescriptor + parameters else resDescriptor
        val triggers = mutable.Map.empty[String, Array[String]]
        val hasTrigger1 = (trigger1 != null) && (trigger1Sql != null)
        val hasTrigger2 = (trigger2 != null) && (trigger2Sql != null)
        val hasTriggers = hasTrigger1 || hasTrigger2
        if (hasTrigger1) {
            val key1 = trigger1.toLowerCase
            val value1 = trigger1Sql.replaceAll( """\\'""", """'""").split( """\\;""").filter(!_.trim.isEmpty)
            triggers += ((key1, value1))
        }
        if (hasTrigger2) {
            val key2 = trigger2.toLowerCase
            val value2 = trigger2Sql.replaceAll( """\\'""", """'""").split( """\\;""").filter(!_.trim.isEmpty)
            triggers += ((key2, value2))
        }
        try {
            val statement = if (hasTriggers) {
                val connection = DriverManager.getConnection(jdbcURL)
                connection.createStatement()
            } else {
                null
            }
            triggers.get("enter") match {
                case Some(sqlsOnEnter) => sqlsOnEnter.foreach(sql => statement.execute(sql))
                case None =>
            }
            df.insertIntoJDBC(jdbcURL, tableName, overwrite = false)
            triggers.get("exit") match {
                case Some(sqlsonExit) => sqlsonExit.foreach(sql => statement.execute(sql))
                case None =>
            }
        } catch {
            case e: SQLException => e.printStackTrace()
        }
        hqlContext.emptyDataFrame
    }

    def writeES (hqlContext : HiveContext, esURL: String, hql : String) : DataFrame = {
        val df = hqlContext.sql(hql)
        val esCluster = esURL match {
            case esURLExtractor(cluster) => cluster
        }
        val dfColumns = df.columns

        df.rdd.foreachPartition(part => {
            val node = nodeBuilder().clusterName(esCluster).client(true).node()
            val client = node.client()
            part.foreach(element => {
                val json = (for (ix <- 0 until dfColumns.length) yield {
                    (dfColumns(ix), element(ix))
                }).toMap
                client.prepareIndex().setSource(json).execute().actionGet()
            })
            node.close()
        })
        hqlContext.emptyDataFrame
    }

    def executeHql(hqlContext: HiveContext, hql : String): DataFrame = {
        hql match {
            case jdbcMatcher(jdbcPath, trigger1, trigger1Sql, trigger2, trigger2Sql, sql) =>
                writeJDBC(hqlContext, jdbcPath, trigger1, trigger1Sql, trigger2, trigger2Sql, sql)
            case metaqMatcher(metaqURL, sql) => writeMetaQ(hqlContext, metaqURL, sql)
            case esMatcher(esURL, sql)       => writeES(hqlContext, esURL, sql)
            case _ => hqlContext.sql(hql)
        }
    }

    def createStreamingContext(checkpointDirectory: String,
                               transHqlPath: Option[String],
                               lowLatencyHqlPath: Option[String],
                               highLatencyHqlPath: Option[String]): StreamingContext = {
        val appName  = getProperty("anystream.spark.appName", Some("AnySteam-ETL"))
        val streamingInterval = getProperty("anystream.spark.streaming.interval", None).toLong
        val metaqZkConnect = getProperty("anystream.metaq.zkConnect", None)
        val metaqTopic  = getProperty("anystream.metaq.topic", None)
        val metaqGroup  = getProperty("anystream.metaq.group", None)
        val metaqRunner = getProperty("anystream.metaq.runners", Some(5.toString)).toInt
        val checkpointInterval = getProperty("anystream.spark.streaming.checkpointInterval", Some("5")).toInt
        val lowLatencyStreamPartitions  = getProperty("anystream.lowLatency.partitions",  Some("2")).toInt
        val highLatencyStreamPartitions = getProperty("anystream.highLatency.partitions", Some("2")).toInt
        val lowLatencyStreamWindow  = getProperty("anystream.lowLatency.window", Some("1:1"))
                .split(":")
                .map(_.trim)
                .padTo(2, "1")
                .map(str => if (str.isEmpty) 1L else str.toLong)
                .map(_ * streamingInterval)
                .map(Seconds(_))
        val highLatencyStreamWindow = getProperty("anystream.highLatency.window",Some("1:1"))
                .split(":")
                .map(_.trim)
                .padTo(2, "1")
                .map(str => if (str.isEmpty) 1L else str.toLong)
                .map(_ * streamingInterval)
                .map(Seconds(_))

        val sparkConf = if (!appName.trim.isEmpty) {
            new SparkConf().setAppName(appName.trim)
        } else {
            new SparkConf()
        }

        val ssc = new StreamingContext(sparkConf, Seconds(streamingInterval))
        val messages = ssc.receiverStream(new MetaQReceiver(metaqZkConnect, metaqTopic, metaqGroup, metaqRunner))
        val asDFStream =  messages.map(msg => {
            try {
                val load = ASMessage.ASDataFrame.parseFrom(msg.getData)
                val (interface, magic, partition, config_id, send_timestamp) =
                    (load.getInterface, load.getMagic, load.getPartition, load.getConfigId, load.getSendTimestamp)
                val data = load.getData.toByteArray
                val msgId = ("__msgId__", msg.getId.toString)
                val ext_domain = load.getExtDomainList.asScala.map(entry => (entry.getKey, entry.getValue)).toMap
                Load(interface, magic, partition, data, ext_domain + msgId, config_id, send_timestamp)
            } catch{
                case e: InvalidProtocolBufferException =>
                    logWarning("invalid message format : " + e.getStackTraceString)
                    Load(null, null, null, null, null, null, null)
                case ex: Throwable => throw ex
            }
        })

        var schema : StructType = null
        val transHqls = transHqlPath match {
            case Some(path) => parseHql(path)
            case None => List(("""_""", """SELECT * FROM `__root__`"""))
        }
        val base = asDFStream.transform(rdd => {
            val hqlContext = getInstance(rdd.sparkContext)
            import hqlContext.implicits._

            var result : DataFrame = null
            val df = rdd.toDF()
//            val interfaces = df.select($"interface").distinct.collect().map(_.getString(0))
//            for (interface <- interfaces) {
//                df.filter($"interface" <=> interface).registerTempTable(interface + "_asDF")
//            }
            df.registerTempTable("__root__")
            for ((tableName, hql) <- transHqls if !hql.trim.toLowerCase.startsWith("insert")) {
                val tblDF = executeHql(hqlContext, hql) // hqlContext.sql(hql)
                if (tableName != "_") {
                    tblDF.registerTempTable(tableName)
                }
                result = tblDF
            }
            schema = result.schema
            result.rdd
        })

        base.checkpoint(Seconds(checkpointInterval * streamingInterval))

        val asActionConfig = List(
            (lowLatencyHqlPath,  lowLatencyStreamWindow,  lowLatencyStreamPartitions,  "__llbase__"),
            (highLatencyHqlPath, highLatencyStreamWindow, highLatencyStreamPartitions, "__hlbase__")
        )
        for ( (actionHqlPath, actionStreamWindow, actionPartitions, baseTableName) <- asActionConfig) {
            actionHqlPath match {
                case Some(path) =>
                    val actionHqls = parseHql(path)
                    val (windowDuration, slidesDuration) = (actionStreamWindow(0), actionStreamWindow(1))
                    val actionStream = base
                            .window(windowDuration, slidesDuration)
                            .repartition(actionPartitions)
                    actionStream.foreachRDD(rdd => {
                        val hqlContext = getInstance(rdd.sparkContext)

                        val df = hqlContext.createDataFrame(rdd, schema)
                        df.registerTempTable(baseTableName)
                        for ((tableName, hql) <- actionHqls) {
                            val tblDF = executeHql(hqlContext, hql) // hqlContext.sql(hql)
                            if (tableName != "_") {
                                tblDF.registerTempTable(tableName)
                            }
                        }
                    })
                case None =>
            }
        }

        ssc.checkpoint(checkpointDirectory)
        ssc
    }

    def setEnv(path: String) : Unit = {
        val propertiesMatcher = """^\s*([^\s]+)\s+(.*)""".r
        val lines = Source.fromFile(path).getLines().toList
        val properties = for (line <- lines.map(_.trim).filter(!_.startsWith("//"))) yield {
            line match {
                case propertiesMatcher(key, value) => (key.trim, value.trim)
                case _ => (null, null)
            }
        }
        for (entity <- properties.filter(_._1 != null)) {
            System.setProperty(entity._1, entity._2)
        }
    }

    def main(args : Array[String]) {
        val optsConf = if (args.length != 0) {
            new ArgsOptsConf(args)
        } else {
            new ArgsOptsConf(List("--help"))
        }
        val confPath = optsConf.confPath.get.get
        val transHqlPath  = optsConf.transHqlPath.get
        val lowLatencyHqlPath = optsConf.lowLatencyHqlPath.get
        val highLatencyHqlPath = optsConf.highLatencyHqlPath.get
        setEnv(confPath)
        val checkpointDirectory = getProperty("anystream.spark.streaming.checkpointDir", None)
        val ssc = StreamingContext.getOrCreate(checkpointDirectory, () =>
            createStreamingContext(checkpointDirectory, transHqlPath, lowLatencyHqlPath, highLatencyHqlPath))

        Signal.handle(new Signal("TERM"), new SignalHandler {
            override def handle(signal: Signal): Unit = {
                val sc = ssc.sparkContext
                log.info("Stopping gracefully Spark Streaming!")
                ssc.stop(stopSparkContext = false, stopGracefully = true)
                log.info("Spark Stream has gracefully stopped")
                sc.stop()
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
