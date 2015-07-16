package com.meizu.anystream


import java.nio.charset.Charset

//import org.apache.spark.rdd.RDD
import sun.misc.{Signal, SignalHandler}
import java.util.zip.GZIPInputStream
import java.sql.{Statement, SQLException, Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone
import java.util.Properties
import com.google.protobuf.InvalidProtocolBufferException
import org.apache.spark.sql.types.{StringType, StructType}

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
import com.meizu.anystream.util.Compression

//import org.elasticsearch.node.NodeBuilder._
//import org.elasticsearch.common.settings.ImmutableSettings
//import org.elasticsearch.client.transport.TransportClient

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
    val debugMode : ScallopOption[Boolean]  = toggle(name = "debug", default = Some(false), descrYes ="debug mode")
//    val hqlPath : ScallopOption[String] = trailArg[String](required = true, name = "hql-script", descr = "hive sql file to execute")
}

case class Load (
    interface:String,
    magic:java.lang.Integer,
    partition:String,
    data:Array[Array[Byte]],
    ext_domain:Map[String,String],
    config_id:java.lang.Integer,
    send_timestamp:java.lang.Long
)

/**
 * @author ${user.name}
 */
object  ETL extends Logging {
    @transient private var instance: HiveContext = null
    final val charset = Charset.forName("UTF-8")
    final val metaqMatcher = (
            """(?s)^\s*[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+""" +
            """[dD][iI][rR][eE][cC][tT][oO][rR][yY]\s+""" +
            """'([mM][eE][tT][aA][qQ]:.*?)'\s+(.*)""").r
    final val jdbcInsertMatcher = (
            """(?s)^\s*[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+[tT][aA][bB][lL][eE]\s+""" + """(.+?)\s+""" +
            """[uU][sS][iI][nN][gG]\s+'([jJ][dD][bB][cC]:.*?)'\s*""" +
            """(?:[sS][qQ][lL]\s+[oO][nN]\s+([eE][nN][tT][eE][rR]|[eE][xX][iI][tT])\s*'((?:[^']|\\')*)'\s*)?""" +
            """(?:[sS][qQ][lL]\s+[oO][nN]\s+([eE][nN][tT][eE][rR]|[eE][xX][iI][tT])\s*'((?:[^']|\\')*)'\s*)?""" +
            """((?:[sS][eE][lL][eE][cC][tT]|[wW][iI][tT][hH]).*)""").r
    final val jdbcUpdateMatcher = (
            """(?s)^\s*[uU][pP][dD][aA][tT][eE]\s+[tT][aA][bB][lL][eE]\s+""" +
            """(.+?)\s+""" + """[sS][eE][tT](.+?)\s+""" +
            """[uU][sS][iI][nN][gG]\s+'([jJ][dD][bB][cC]:.*?)'\s+""" +
            """(?:""" + """[cC][aA][lL][lL][bB][aA][cC][kK]\s+[oO][nN]\s+[nN][oO][uU][pP][dD][aA][tT][eE]\s*""" +
            """'((?:[^']|\\')*)'\s*""" + """)?""" +
            """((?:[sS][eE][lL][eE][cC][tT]|[wW][iI][tT][hH]).*)""").r
    final val esMatcher = (
            """(?s)^\s*[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+""" +
            """[dD][iI][rR][eE][cC][tT][oO][rR][yY]\s+""" +
            """'([eE][sS]:.*?)'\s+(.*)""").r
    final val jsonMatcher = (
            """(?s)^\s*[cC][rR][eE][aA][tT][eE]\s+[tT][eE][mM][pP][oO][rR][aA][rR][yY]\s+[tT][aA][bB][lL][eE]\s+""" +
            """(\S+)\s+[uU][sS][iI][nN][gG]\s+org\.apache\.spark\.sql\.json\s+[aA][sS]\s+(.*)""").r

    final val metaqURLExtractor = """[mM][eE][tT][aA][qQ]://([^/]*)/([^/\?]*)(?:\?(.*))?""".r
//    final val jdbcURLExtractor = """(.*)/([^/\?]*)(\?.*)?""".r
    final val esURLExtractor = """[eE][sS]://([^/\?]*)""".r
    final val dayPartitionMatcher = """([\d]*)?[d|D]""".r
    final val hourPartitionMatcher = """([\d]*)?[h|H]""".r
    final val minutePartitionMatcher = """([\d]*)?[m|M]""".r

    final var isDebugMode = false


    // Instantiate HiveContext on demand
    def getInstance(sparkContext: SparkContext): HiveContext = synchronized {
        if (instance == null) {
            instance = new HiveContext(sparkContext)
            instance.udf.register("partitioner", (partition: String, timestamp: Long) => {
                val offset = TimeZone.getDefault.getOffset(timestamp)
                val biasTimestamp = timestamp + offset
                partition match {
                    case dayPartitionMatcher(dInterval) =>
                        val interval = if (dInterval != null && !dInterval.isEmpty) {
                            (if (dInterval.toInt == 0) 1 else dInterval.toInt) * 24 * 3600 * 1000L
                        } else {
                            24 * 3600 * 1000L
                        }
                        val ts = (biasTimestamp - biasTimestamp % interval) - offset
                        val dt = new Date(ts)
                        val dtFormatter = new SimpleDateFormat("yyyyMMdd")
                        new java.lang.Long(dtFormatter.format(dt).toLong)
                    case hourPartitionMatcher(hInterval) =>
                        val interval = if (hInterval != null && !hInterval.isEmpty) {
                            (if (hInterval.toInt == 0) 1 else hInterval.toInt) * 3600 * 1000L
                        } else {
                            3600 * 1000L
                        }
                        val ts = (biasTimestamp - biasTimestamp % interval) - offset
                        val dt = new Date(ts)
                        val dtFormatter = new SimpleDateFormat("yyyyMMddHH")
                        new java.lang.Long(dtFormatter.format(dt).toLong)
                    case minutePartitionMatcher(mInterval) =>
                        val interval = if (mInterval != null && !mInterval.isEmpty) {
                            (if (mInterval.toInt == 0) 1 else mInterval.toInt) * 60 * 1000L
                        } else {
                            60 * 1000L
                        }
                        val ts = (biasTimestamp - biasTimestamp % interval) - offset
                        val dt = new Date(ts)
                        val dtFormatter = new SimpleDateFormat("yyyyMMddHHmm")
                        new java.lang.Long(dtFormatter.format(dt).toLong)
                    case _ => null
                }
            })
            instance.udf.register("str_to_array_of_map",
                (str: String, arrayElementDelimiter: String, mapElementDelimiter: String, keyValDelimiter: String) => {
                    if (str != null) {
                        str.split(arrayElementDelimiter).filter(!_.trim.isEmpty).map(eleStr => {
                            eleStr.split(mapElementDelimiter).filter(!_.trim.isEmpty)
                                    .map(_.split(keyValDelimiter).padTo(2, ""))
                                    .map(arr => (arr(0), arr(1)))
                                    .toMap
                        })
                    } else {
                        null
                    }
            })
            instance.udf.register("utf8", (bytes : Array[Byte]) => new String(bytes, charset))
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

        val result = for ( hql <- hqlList) yield {
            val tagHql = hql.split(":=")
            if (tagHql.length == 1) {
                ("_", tagHql(0).trim)
            } else {
                val identifier = tagHql(0).trim
                val hql = tagHql(1).trim
                if (!isDebugMode && identifier.endsWith("_d_")) {
                    ("", "")
                } else {
                    (identifier, hql)
                }
            }
        }

        result.filter(!_._1.trim.isEmpty)
    }

    def writeMetaQ(hqlContext : HiveContext, metaqURL : String, hql : String) : DataFrame = {
        val df = hqlContext.sql(hql)
        val (zkCluster,  topic) = metaqURL match {
            case metaqURLExtractor(zkClusterAddr, topicName, parameters) => (zkClusterAddr, topicName)
        }
        val metaqWriterRef = MetaQWriter(zkCluster, topic)
        df.rdd.foreachPartition(part => {
            var isInException = true
            val metaqWriter = metaqWriterRef.copy()
            while (isInException) {
                try {
                    metaqWriter.init()
                    part.foreach(element => {
                        val data = element match {
                            case Row(col1: Array[Byte]) => col1
                            case _ => null
                        }
                        if (data != null) {
                            val msg = new Message(metaqWriter.getTopic, data) // element.toString().getBytes
                            val sendResult = metaqWriter.sendMessage(msg)
                            if (!sendResult.isSuccess) {
                                logError("Send message failed,error message:" + sendResult.getErrorMessage)
                            }
                        }
                    })
                    isInException = false
                } catch {
                    case e: MetaClientException =>
                        isInException = true
                        logError("Send message exception : " + e.getStackTraceString)
                } finally {
                    metaqWriter.close()
                }

                if (isInException) {
                    try {
                        Thread.sleep(3000)
                    } catch {
                        case e: InterruptedException =>
                    }
                    logError("trying to resend message ...")
                }
            }
        })
        hqlContext.emptyDataFrame
    }

    def writeJDBC(hqlContext : HiveContext,
                tableName: String,
                jdbcURLStr : String,
                trigger1 : String,
                trigger1Sql : String,
                trigger2 : String,
                trigger2Sql : String,
                hql : String): DataFrame = {
        val df = hqlContext.sql(hql)
        val jdbcURL = jdbcURLStr.replaceAll("""\\;""", """;""")

        if (jdbcURL.trim.toLowerCase.startsWith("jdbc:phoenix")) {
            val phoenix = jdbcURL.trim.split( """:""")    // ["jdbc", "phoenix", "zkhost1,zhhost2", "zkClientPort", ...]
            val properties = Map("table" -> tableName, "zkUrl" -> (phoenix(2) + ":" + phoenix(3)))
            try {
                df.save("org.apache.phoenix.spark", SaveMode.Overwrite, properties)
//                df.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).options(properties).save()
            } catch {
                case e: Exception => logWarning("insert into hbase via phoenix : ", e)
            }
            return hqlContext.emptyDataFrame
        }

        var conn : Connection = null
        var stmt : Statement = null
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
            if (hasTriggers) {
                conn = DriverManager.getConnection(jdbcURL)
                stmt = conn.createStatement()
            }
            triggers.get("enter") match {
                case Some(sqlsOnEnter) => sqlsOnEnter.foreach(sql => stmt.execute(sql))
                case None =>
            }
            df.insertIntoJDBC(jdbcURL, tableName, overwrite = false)
//            df.write.mode(SaveMode.Append).jdbc(jdbcURL, tableName, new Properties)
            triggers.get("exit") match {
                case Some(sqlsonExit) => sqlsonExit.foreach(sql => stmt.execute(sql))
                case None =>
            }
        } catch {
            case e: SQLException => logWarning("insert into database via jdbc : ", e)
        } finally {
            if (stmt != null) stmt.close()
            if (conn != null) conn.close()
        }
        hqlContext.emptyDataFrame
    }

    def updateJDBC(hqlContext : HiveContext,
                   tableName : String,
                   updateSQLStr : String,
                   jdbcURLStr : String,
                   callbackSQL : String,
                   hql : String) : DataFrame = {
        val df = hqlContext.sql(hql)
        val jdbcURL = jdbcURLStr.replaceAll("""\\;""", """;""")

        val (updateSqlPrefix, updateSQL) = if (jdbcURL.trim.toLowerCase.startsWith("jdbc:phoenix")) {
            (s"UPSERT INTO $tableName", " " + updateSQLStr)
        } else {
            (s"UPDATE $tableName ", "SET " + updateSQLStr)
        }
        val callback = if (callbackSQL != null) {
            callbackSQL.replaceAll( """\\'""", """'""").split( """\\;""").filter(!_.trim.isEmpty)
        } else {
            null
        }
        val schema = df.schema.fields
        df.rdd.foreachPartition(part => {
            var conn : Connection = null
            var stmt : Statement = null
//            var committed = false
            try {
                conn = DriverManager.getConnection(jdbcURL)
                conn.setAutoCommit(true)
                stmt = conn.createStatement()
                part.foreach(row => {
                    val rowList = (
                        for (ix <- 0 until row.length) yield {
                            schema(ix).dataType match {
                                case StringType =>
                                    if(row.isNullAt(ix)) {
                                        "NULL"
                                    } else {
                                        "'" + row(ix).toString.replaceAll("""'""", """\\'""") + "'"
                                    }
                                case _ => if (row.isNullAt(ix)) "NULL" else row(ix).toString
                            }
                        }
                    ).toList
                    val updateStatement = updateSqlPrefix +
                        updateSQL.replaceAll("""((?<!%)%\d+)""", """$1\$s""").format(rowList: _*)
                    val updatedRows = stmt.executeUpdate(updateStatement)
                    if (updatedRows == 0 && callback != null) {
                        callback.map(str =>
                            str.replaceAll("""((?<!%)%\d+)""", """$1\$s""").format(rowList: _*)
                        ).foreach(sql => stmt.execute(sql))
                    }
                })
//                conn.commit()
//                committed = true
            } catch {
                case e: SQLException => logWarning("update JDBC exception : ", e)
            }finally {
                if (stmt != null) stmt.close()
                if (conn != null) conn.close()
//                if (!committed) {
//                    // The stage must fail.  We got here through an exception path, so
//                    // let the exception through unless rollback() or close() want to
//                    // tell the user about another problem.
//                    try {
//                        conn.rollback()
//                        conn.close()
//                    } catch {
//                        case e: Exception => logWarning("Transaction failed", e)
//                    }
//                } else {
//                    // The stage must succeed.  We cannot propagate any exception close() might throw.
//                    try {
//                        conn.close()
//                    } catch {
//                        case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
//                    }
//                }
            }
        })
        hqlContext.emptyDataFrame
    }

    def writeES (hqlContext : HiveContext, esURL: String, hql : String) : DataFrame = {
//        val df = hqlContext.sql(hql)
//        val esCluster = esURL match {
//            case esURLExtractor(cluster) => cluster
//        }
//        val dfColumns = df.columns
//        val esIndex = ""
//        val esType = ""
//
//        df.toJSON.foreachPartition(part =>{
//            val settings = ImmutableSettings.settingsBuilder.put("cluster.name", esCluster).build
//            val client = new TransportClient(settings)
//                .addTransportAddress("172.16.82.131", 9300)
//            part.foreach(json => {
//                client.prepareIndex(esIndex, esType).setSource(json).execute().actionGet()
//            })
//            client.close()
//        })

//        df.foreachPartition(part => {
//            val node = nodeBuilder().clusterName(esCluster).client(true).node()
//            val client = node.client()
//            part.foreach(element => {
//                val json = (for (ix <- 0 until dfColumns.length) yield {
//                    (dfColumns(ix), element(ix))
//                }).toMap
//                client.prepareIndex(esIndex, esType).setSource(json).execute().actionGet()
//            })
//            node.close()
//        })
        hqlContext.emptyDataFrame
    }

    def createJsonTable(hqlContext: HiveContext, tableName : String, hql : String) : DataFrame = {
        val df = hqlContext.sql(hql)
        val stringRDD = df.rdd.map(_.getString(0))
        val jsonDF = hqlContext.jsonRDD(stringRDD)
//        val jsonDF = hqlContext.read.json(stringRDD)
        jsonDF.registerTempTable(tableName)
        jsonDF
    }

    def executeHql(hqlContext: HiveContext, hql : String): DataFrame = {
        hql match {
            case jdbcInsertMatcher(tableName, jdbcURL, trNo1, trNo1Sql, trNo2, trNo2Sql, sql) =>
                writeJDBC(hqlContext, tableName, jdbcURL, trNo1, trNo1Sql, trNo2, trNo2Sql, sql.trim)
            case jdbcUpdateMatcher(tableName, updateSql, jdbcURL, callbackSql, sql) =>
                updateJDBC(hqlContext, tableName, updateSql, jdbcURL, callbackSql, sql.trim)
            case metaqMatcher(metaqURL, sql) => writeMetaQ(hqlContext, metaqURL, sql)
            case jsonMatcher(jsonTableName, sql) => createJsonTable(hqlContext, jsonTableName, sql.trim)
//            case esMatcher(esURL, sql)       => writeES(hqlContext, esURL, sql)
            case _ => hqlContext.sql(hql)
        }
    }

    def createStreamingContext(checkpointDirectory: String,
                               transHqlPath: Option[String],
                               lowLatencyHqlPath: Option[String],
                               highLatencyHqlPath: Option[String]): StreamingContext = {
        val appName  = getProperty("anystream.spark.appName", Some("AnySteam-ETL"))
        val streamingInterval = Math.abs(getProperty("anystream.spark.streaming.interval", None).toLong)
        val metaqZkConnect = getProperty("anystream.metaq.zkConnect", None)
        val metaqTopic  = getProperty("anystream.metaq.topic", None)
        val metaqGroup  = getProperty("anystream.metaq.group", None)
        val metaqRunner = Math.abs(getProperty("anystream.metaq.runners", Some(5.toString)).toInt)
        val checkpointInterval = Math.abs(getProperty("anystream.spark.streaming.checkpointInterval", Some("5")).toInt)
        val transformStreamPartitions   = Math.abs(getProperty("anystream.transform.partitions", Some("0")).toInt)
        val lowLatencyStreamPartitions  = Math.abs(getProperty("anystream.lowLatency.partitions",  Some("0")).toInt)
        val highLatencyStreamPartitions = Math.abs(getProperty("anystream.highLatency.partitions", Some("0")).toInt)
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
        val msgStream = if ( transformStreamPartitions == 0) {
            messages
        } else {
            messages.repartition(transformStreamPartitions)
        }
        val asDFStream =  msgStream.map(msg => {
            try {
                val load = ASMessage.ASDataFrame.parseFrom(msg.getData)
                val (interface, magic, partition, config_id, send_timestamp) =
                    (load.getInterface, load.getMagic, load.getPartition, load.getConfigId, load.getSendTimestamp)
                val compressionType = load.getCompression
                val data = compressionType match {
                    case 0 =>
                        new String(load.getData.toByteArray, charset).split("\n").map(str => {
                            str.replace(0x01.toChar, 0x11.toChar)
                               .replace(0x02.toChar, 0x12.toChar)
                               .replace(0x03.toChar, 0x13.toChar)
                               .replace(0x04.toChar, 0x14.toChar)
                               .replace(0x05.toChar, 0x15.toChar)
                               .replace(0x06.toChar, 0x16.toChar)
                               .replace(0x07.toChar, 0x17.toChar)
                               .getBytes(charset)
                        })
                    case 1 =>
                        val uncompressBytes = Compression.uncompress[GZIPInputStream](load.getData.toByteArray)
                        new String(uncompressBytes, charset).split("\n").map(str => {
                            str.replace(0x01.toChar, 0x11.toChar)
                               .replace(0x02.toChar, 0x12.toChar)
                               .replace(0x03.toChar, 0x13.toChar)
                               .replace(0x04.toChar, 0x14.toChar)
                               .replace(0x05.toChar, 0x15.toChar)
                               .replace(0x06.toChar, 0x16.toChar)
                               .replace(0x07.toChar, 0x17.toChar)
                               .getBytes(charset)
                        })
                    case _ =>
                        logError("unknown data compression format "
                                + s"(msgId : ${msg.getId.toString}, msgInterface : ${load.getInterface})" )
                        Array.empty[Array[Byte]]
                }
                val msgId = ("__msgId__", msg.getId.toString)
                val ext_domain = load.getExtDomainList.asScala.map(entry => (entry.getKey, entry.getValue)).toMap
                Load(interface, magic, partition, data, ext_domain + msgId, config_id, send_timestamp)
            } catch{
                case e: InvalidProtocolBufferException =>
                    logWarning("invalid message format : " + e.getStackTraceString)
                    Load(null, null, null, null, null, null, null)
                case ex: Throwable =>
                    logWarning("unknown exception : " + ex.getStackTraceString)
                    Load(null, null, null, null, null, null, null)
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
                    val npartions = Math.abs(hqlContext.getConf("anystream.dataframe.partitions", "0").toInt)
                    if (npartions == 0) {
                        tblDF.registerTempTable(tableName)
                    } else {
                        tblDF.coalesce(npartions).registerTempTable(tableName)
                    }
                }
                result = tblDF
            }
            schema = result.schema
            result.rdd
        })

//        base.checkpoint(Seconds(checkpointInterval * streamingInterval))

        val asActionConfig = List(
            (lowLatencyHqlPath,  lowLatencyStreamWindow,  lowLatencyStreamPartitions,  "__llbase__"),
            (highLatencyHqlPath, highLatencyStreamWindow, highLatencyStreamPartitions, "__hlbase__")
        )
        for ( (actionHqlPath, actionStreamWindow, actionPartitions, baseTableName) <- asActionConfig) {
            actionHqlPath match {
                case Some(path) =>
                    val actionHqls = parseHql(path)
                    val (windowDuration, slidesDuration) = (actionStreamWindow(0), actionStreamWindow(1))
                    val actionStream = if (actionPartitions == 0) {
                        base.window(windowDuration, slidesDuration)
                    } else {
                        base.window(windowDuration, slidesDuration).repartition(actionPartitions)
                    }
                    actionStream.foreachRDD(rdd => {
                        val hqlContext = getInstance(rdd.sparkContext)

                        val df = hqlContext.createDataFrame(rdd, schema)
                        df.registerTempTable(baseTableName)
                        for ((tableName, hql) <- actionHqls) {
                            val tblDF = executeHql(hqlContext, hql) // hqlContext.sql(hql)
                            if (tableName != "_") {
                                val npartions = Math.abs(hqlContext.getConf("anystream.dataframe.partitions", "0").toInt)
                                if (npartions == 0) {
                                    tblDF.registerTempTable(tableName)
                                } else {
                                    tblDF.coalesce(npartions).registerTempTable(tableName)
                                }
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
        isDebugMode = optsConf.debugMode.get.get
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
