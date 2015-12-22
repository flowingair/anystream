package com.meizu.anystream

import java.net.URI
import java.io.ByteArrayInputStream

import sun.misc.{BASE64Encoder, Signal, SignalHandler}
import java.nio.charset.Charset
import java.security.MessageDigest
import java.util.zip.GZIPInputStream
import java.sql.{Statement, PreparedStatement, SQLException, Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone
import java.util.Properties

import scala.collection.mutable.{LinkedHashSet, ArrayBuffer, ArrayBuilder}
import scala.io.Source
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException

import org.rogach.scallop.{ScallopOption, ScallopConf}

import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{TaskContext, SparkContext, SparkConf, Logging}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal

import com.taobao.metamorphosis.Message
import com.taobao.metamorphosis.exception.MetaClientException

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
    final val hdfsMatcher = (
            """(?s)^\s*[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+""" +
            """[dD][iI][rR][eE][cC][tT][oO][rR][yY]\s+""" +
            """'([hH][dD][fF][sS]:.*?)'\s+(.*)""").r
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
    final val jdbcLoadMatcher = (
            """(?s)^\s*[iI][mM][pP][oO][rR][tT]\s+[tT][aA][bB][lL][eE]\s+(.+?)\s+[fF][rR][oO][mM]\s+(.+?)\s+""" +
            """[uU][sS][iI][nN][gG]\s+'([jJ][dD][bB][cC]:.*?)'\s*""").r

    final val metaqURLExtractor = """[mM][eE][tT][aA][qQ]://([^/]*)/([^/\?]*)(?:\?(.*))?""".r
    final val hdfsURLExtractor  = """([^:/]+://)([^/]*)([^\?]*)(?:\?(.*))?""".r
    final val esURLExtractor = """[eE][sS]://([^/\?]*)""".r
    final val dayPartitionMatcher = """([\d]*)?[d|D]""".r
    final val hourPartitionMatcher = """([\d]*)?[h|H]""".r
    final val minutePartitionMatcher = """([\d]*)?[m|M]""".r

    final var isDebugMode = false

    final var mergeFileName: String = null

    final val fsSinkPaths =  scala.collection.mutable.Map.empty[String, Array[String]]


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
                        str.split(arrayElementDelimiter).filter(_.trim.nonEmpty).map(eleStr => {
                            eleStr.split(mapElementDelimiter).filter(_.trim.nonEmpty).map(str => {
                                val arr = str.split(keyValDelimiter).padTo(2, "")
                                (arr(0), arr(1))
                            }).toMap
                        })
                    } else {
                        null
                    }
            })
            instance.udf.register("utf8", (bytes : Array[Byte]) => {
                if (bytes != null ) new String(bytes, charset) else null
            })
            instance.udf.register("filter_not", (event: Map[String, String], keySeq: Seq[String]) => {
                if (event != null) {
                    if (keySeq != null){
                        val keySet = HashSet(keySeq: _*)
                        event.filterKeys(!keySet.contains(_))
                    } else {
                        event
                    }
                } else {
                    null
                }
            })
            instance.udf.register("encrypt", (str: String) => {
                if (str != null) {
                    val key = Array[Byte](-93, -117, -26, -128, 93, -67, -99, 12)
                    val data = str.getBytes(charset)
                    val result = for ( ix <- 0 until data.length) yield {
                        (data(ix) ^ key(ix % 8)).toByte
                    }
                    new String(result.toArray, charset)
                } else {
                    null
                }
            })
            instance.udf.register("map_to_str",
                (mapObj: Map[String,String], mapElementDelimiter: String, keyValDelimiter: String) => {
                    if (mapObj != null) {
                        mapObj.map(pair => pair._1 + keyValDelimiter + pair._2).mkString(mapElementDelimiter)
                    } else {
                        null
                    }
            })
            instance.udf.register("md5", (str : String) => {
                if (str != null) {
                    val md = MessageDigest.getInstance("MD5").digest(str.getBytes(charset))
                    md.map(_ & 0xFF).map("%02x".format(_)).mkString
                } else {
                    null
                }
            })
            instance.udf.register("sha1", (str : String) => {
                if (str != null) {
                    val md = MessageDigest.getInstance("SHA-1").digest(str.getBytes(charset))
                    md.map(_ & 0xFF).map("%02x".format(_)).mkString
                } else {
                    null
                }
            })
            instance.udf.register("sha256", (str : String) => {
                if (str != null) {
                    val md = MessageDigest.getInstance("SHA-256").digest(str.getBytes(charset))
                    md.map(_ & 0xFF).map("%02x".format(_)).mkString
                } else {
                    null
                }
            })
            instance.udf.register("as_message", (interfaceName: String, data: String) => {
                val bytes = if (data != null) data.getBytes(charset) else Array.emptyByteArray
                ASMessage.ASDataFrame.newBuilder()
                    .setInterface(interfaceName)
                    .setData(ByteString.copyFrom(bytes))
                    .setSendTimestamp(System.currentTimeMillis())
                    .setCompression(0)
                    .build().toByteArray
            })
            instance.udf.register("array_bytes", (seq : scala.collection.Seq[Array[Byte]]) => {
                if ( seq != null) {
                    seq.map(_.size).sum
                } else {
                    0
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
        val npartions = Math.abs(hqlContext.getConf("anystream.dataframe.partitions", "0").toInt)
        val df = if (npartions == 0) { hqlContext.sql(hql) } else { hqlContext.sql(hql).coalesce(npartions) }
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
        val npartions = Math.abs(hqlContext.getConf("anystream.dataframe.partitions", "0").toInt)
        val df = if (npartions == 0) { hqlContext.sql(hql) } else { hqlContext.sql(hql).coalesce(npartions) }
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
//            df.insertIntoJDBC(jdbcURL, tableName, overwrite = false)
            df.write.mode(SaveMode.Append).jdbc(jdbcURL, tableName, new Properties)
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

    private def setStatement(stmt: PreparedStatement, setRule : List[(Int, Int)], row : Row): Unit = {
        val schema = row.schema.fields
        for ((setIndex, rowIndex) <- setRule){
            schema(rowIndex).dataType match {
                case IntegerType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.INTEGER)
                    } else {
                        stmt.setInt(setIndex, row.getInt(rowIndex))
                    }
                case LongType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.BIGINT)
                    } else {
                        stmt.setLong(setIndex, row.getLong(rowIndex))
                    }
                case DoubleType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.DOUBLE)
                    } else {
                        stmt.setDouble(setIndex, row.getDouble(rowIndex))
                    }
                case FloatType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.REAL)
                    } else {
                        stmt.setFloat(setIndex, row.getFloat(rowIndex))
                    }
                case ShortType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.INTEGER)
                    } else {
                        stmt.setInt(setIndex, row.getShort(rowIndex))
                    }
                case ByteType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.INTEGER)
                    } else {
                        stmt.setInt(setIndex, row.getByte(rowIndex))
                    }
                case BooleanType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.BIT)
                    } else {
                        stmt.setBoolean(setIndex, row.getBoolean(rowIndex))
                    }
                case StringType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.CLOB)
                    } else {
                        stmt.setString(setIndex, row.getString(rowIndex))
                    }
                case BinaryType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.BLOB)
                    } else {
                        stmt.setBytes(setIndex, row.getAs[Array[Byte]](rowIndex))
                    }
                case TimestampType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.TIMESTAMP)
                    } else {
                        stmt.setTimestamp(setIndex, row.getAs[java.sql.Timestamp](rowIndex))
                    }
                case DateType =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.DATE)
                    } else {
                        stmt.setDate(setIndex, row.getAs[java.sql.Date](rowIndex))
                    }
                case DecimalType.Unlimited =>
                    if (row.isNullAt(rowIndex)) {
                        stmt.setNull(setIndex, java.sql.Types.DECIMAL)
                    } else {
                        stmt.setBigDecimal(setIndex, row.getAs[java.math.BigDecimal](rowIndex))
                    }
                case _ => throw new IllegalArgumentException(
                    s"Can't translate non-null value for field $rowIndex")
            }
        }
    }

    def updateJDBC(hqlContext : HiveContext,
                   tableName : String,
                   updateSQLStr : String,
                   jdbcURLStr : String,
                   callbackSQLStr : String,
                   hql : String) : DataFrame = {
        val npartions = Math.abs(hqlContext.getConf("anystream.dataframe.partitions", "0").toInt)
        val df = if (npartions == 0) { hqlContext.sql(hql) } else { hqlContext.sql(hql).coalesce(npartions) }

        val formatStub = """((?<!%)%\d+)""".r
        val formatCtor = """\?"""

        val jdbcURL = jdbcURLStr.replaceAll("""\\;""", """;""")
        val isPhoenixDriver = jdbcURL.trim.toLowerCase.startsWith("jdbc:phoenix")
        val updateSQLMatcher = if (isPhoenixDriver) {
            s"UPSERT INTO $tableName" + " " + updateSQLStr
        } else {
            s"UPDATE $tableName " + "SET " + updateSQLStr
        }
        logInfo(s"update external databases $jdbcURL : $updateSQLMatcher")
        val updateSQLPattern = formatStub.replaceAllIn(updateSQLMatcher, formatCtor).format("")
        val updateSQLSetRule = formatStub.findAllIn(updateSQLMatcher).toList.map(_.filter(_.isDigit)).zipWithIndex
                .map(pair => (pair._2 + 1, pair._1.toInt - 1))

        val callbackSQL = if (callbackSQLStr != null) {
            val sqlsList = callbackSQLStr.replaceAll( """\\'""", """'""").split( """\\;""").filterNot(_.trim.isEmpty)
            for ( sqlStr <- sqlsList) yield {
                logInfo(s"callback of updating external databases $jdbcURL : $sqlStr")
                val sqlPattern = formatStub.replaceAllIn(sqlStr, formatCtor).format("")
                val sqlSetRule = formatStub.findAllIn(sqlStr).toList.map(_.filter(_.isDigit)).zipWithIndex
                        .map(pair => (pair._2 + 1, pair._1.toInt - 1))
                (sqlPattern, sqlSetRule)
            }
        } else {
            null
        }

        val isAutoCommit = false // if (isPhoenixDriver && callbackSQLStr != null) true else false
        if (isAutoCommit) {
            logInfo("auto commit has already turned on")
        }

//        val schema = df.schema.fields
//        logInfo("updateJDBC dataframe schema : " + schema.mkString("[", ", ", "]"))
//        val linesPerTransaction = Math.abs(hqlContext.getConf("anystream.jdbc.transaction.size", "0").toLong)
        df.rdd.foreachPartition(part => {
            val conn = DriverManager.getConnection(jdbcURL)
            var committed = false
            var sqlClause = ""
            try {
                if (isAutoCommit) {
                    conn.setAutoCommit(true)
                    committed = true
                } else {
                    conn.setAutoCommit(false)
                    committed = false
                }
                val updateStmt = conn.prepareStatement(updateSQLPattern)
                val callbackStmt = if (callbackSQL != null) {
                    callbackSQL.map(pair => (conn.prepareStatement(pair._1), pair._1, pair._2))
                } else {
                    null
                }
                try {
//                    var lines = 0L
                    while (part.hasNext) {
                        val row = part.next()
                        sqlClause = "prepareStatement : " + updateSQLPattern + "\n" +
                                "rule : " + updateSQLSetRule.mkString("[", ", ", "]") + "\n" +
                                "row : " + row.mkString("[", ", ", "]") + "\n" +
                                "row schema : " + row.schema.fields.mkString("[", ", ", "]")
                        logDebug(s"update content : $sqlClause")
                        setStatement(updateStmt, updateSQLSetRule, row)
                        val updatedRows = updateStmt.executeUpdate()
                        if (updatedRows == 0 && callbackStmt != null) {
                            callbackStmt.foreach(pair => {
                                val (stmt, pattern, rule) = pair
                                sqlClause = "prepareStatement : " + pattern + "\n" +
                                        "rule : " + rule.mkString("[", ", ", "]") + "\n" +
                                        "row : " + row.mkString("[", ", ", "]") + "\n" +
                                        "row schema : " + row.schema.fields.mkString("[", ", ", "]")
                                logDebug(s"callback content : $sqlClause")
                                setStatement(stmt, rule, row)
                                stmt.executeUpdate()
                            })
                            if (isPhoenixDriver && !isAutoCommit) {
                                conn.commit()
                            }
                        }
//                        lines += 1
//                        if (linesPerTransaction != 0 && lines >= linesPerTransaction) {
//                            conn.commit()
//                            lines = 0
//                        }
                    }
                } finally {
                    updateStmt.close()
                    if (callbackStmt != null) {
                        callbackStmt.map(_._1).foreach(_.close())
                    }
                }
                if (!isAutoCommit) {
                    conn.commit()
                    committed = true
                }
            } catch {
                case e: SQLException => logWarning(s"update JDBC exception $sqlClause : ", e)
            }finally {
                if (!committed) {
                    // The stage must fail.  We got here through an exception path, so
                    // let the exception through unless rollback() or close() want to
                    // tell the user about another problem.
                    try {
                        conn.rollback()
                        conn.close()
                    } catch {
                        case e: Exception => logWarning("Transaction failed", e)
                    }
                } else {
                    // The stage must succeed.  We cannot propagate any exception close() might throw.
                    try {
                        conn.close()
                    } catch {
                        case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
                    }
                }
            }
        })
        hqlContext.emptyDataFrame
    }

    def loadFromJDBC(hqlContext : HiveContext,
                     sparkTable : String,
                     jdbcTable : String,
                     jdbcURL : String) : DataFrame = {
        try {
            val df = hqlContext.read.jdbc(jdbcURL, jdbcTable, new Properties())
            df.registerTempTable(sparkTable)
        } catch {
            case e: Exception =>
                if (hqlContext.tableNames().contains(sparkTable)) {
                    logWarning(s"Loading external table $jdbcTable from $jdbcURL fails, use old data")
                } else {
                    throw e
                }
        }
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
        val npartions = Math.abs(hqlContext.getConf("anystream.dataframe.partitions", "0").toInt)
        val df = if (npartions == 0) { hqlContext.sql(hql) } else { hqlContext.sql(hql).coalesce(npartions) }
        val stringRDD = df.map(_.getString(0))
//        val jsonDF = hqlContext.jsonRDD(stringRDD)
        val jsonDF = hqlContext.read.json(stringRDD)
        jsonDF.registerTempTable(tableName)
        hqlContext.cacheTable(tableName)
        logInfo(s"the schema of $tableName : \n" + jsonDF.schema.treeString)
        hqlContext.emptyDataFrame
    }

    private def getTimeTag : String = {
        val ts = System.currentTimeMillis()
        val dt = new Date(ts)
        val dtFormater = new SimpleDateFormat("yyyyMMddHHmmss")
        dtFormater.format(dt)
    }

    private def getPaths(applicationId : String,
                         fs : String,
                         dir: String,
                         argumentList : Map[String, String]) : Array[String] = {
        val argument = argumentList.map(pair => pair._1 + "=" + pair._2).mkString("&")
        val partition = argumentList.getOrElse("partition", "false").toBoolean
        val concurrent = argumentList.getOrElse("concurrent", 2.toString).toInt
        val dstDir = fs + dir + "?" + argument
        val timeTag = getTimeTag
        val ymd = timeTag.take(8)
        fsSinkPaths.get(dstDir) match {
            case Some(oldPaths) => oldPaths
            case None => if (partition) {
                Range(0, concurrent).toArray.map(index => s"$dir/stat_date=$ymd/${applicationId}__${index}_$timeTag")
            } else {
                Range(0, concurrent).toArray.map(index => s"$dir/${applicationId}__${index}_$timeTag")
            }
        }
    }

    private def updatePaths(paths : Array[String],
                            applicationId : String,
                            fs : String,
                            dir: String,
                            argumentList : Map[String, String]) : (Array[String], List[(String, String)]) = {
        val fsURI = URI.create(fs)
        val hadoopConf = new Configuration()
        val filesystem = FileSystem.get(fsURI, hadoopConf)
        val sizeThreshold = argumentList.getOrElse("size_threshold", (1024L * 1024L * 1024L).toString).toLong
        val partition = argumentList.getOrElse("partition", "false").toBoolean
        val timeTag = getTimeTag
        val ymd = timeTag.take(8)
        val newPaths = paths.zipWithIndex.map(pair => {
            val (pathName, index) = pair
            val path = new Path(pathName)
            val isExisted = filesystem.exists(path)
            val isOverSized = isExisted && filesystem.getFileStatus(path).getLen >= sizeThreshold
            val isDayChanged =  partition && pathName.takeRight(14).take(8) != ymd
            val newPathName = if (isOverSized) {
                if (partition) {
                    s"$dir/stat_date=$ymd/${applicationId}__${index}_$timeTag"
                } else {
                    s"$dir/${applicationId}__${index}_$timeTag"
                }
            } else {
                pathName
            }
            val nextBatchPathName = if (isDayChanged) {
                s"$dir/stat_date=$ymd/${applicationId}__${index}_$timeTag"
            } else {
                newPathName
            }
            val isChanged = isOverSized || (isExisted && isDayChanged)
            (nextBatchPathName, isChanged, pathName)
        })
        val deltaPaths = newPaths.filter(_._2).map(_._3)
                .map(pathName => (pathName.takeRight(14).take(8), pathName)).toList
        (newPaths.map(_._1), deltaPaths)
    }

    private def row2string(row : Row, schema : Array[(DataType, Int)], separator : Int = 0x01) : String = {
        val arr = schema.map(pair => {
            val (fieldType, index) = pair
            fieldType match {
                case ByteType    => if (row.isNullAt(index)) """\N""" else row.getByte(index).toString
                case ShortType   => if (row.isNullAt(index)) """\N""" else row.getShort(index).toString
                case IntegerType => if (row.isNullAt(index)) """\N""" else row.getInt(index).toString
                case LongType    => if (row.isNullAt(index)) """\N""" else row.getLong(index).toString
                case FloatType   => if (row.isNullAt(index)) """\N""" else row.getFloat(index).toString
                case DoubleType  => if (row.isNullAt(index)) """\N""" else row.getDouble(index).toString
                case BooleanType => if (row.isNullAt(index)) """\N""" else row.getBoolean(index).toString
                case StringType  => if (row.isNullAt(index)) """\N""" else row.getString(index)
                case BinaryType  => if (row.isNullAt(index)) """\N""" else {
//                    val bytes = row.getSeq[Byte](index).toArray
                    val bytes = row.get(index).asInstanceOf[Array[Byte]]
                    val encoder = new BASE64Encoder()
                    encoder.encode(bytes)
                }
                case ArrayType(eleType, _)   => if (row.isNullAt(index)) """\N""" else {
                    val arr = row.getSeq[Any](index)
                    arr.map(ele => {
                        row2string(Row(ele), Array((eleType, 0)), separator + 1)
                    }).mkString((separator + 1).toChar.toString)
                }
                case MapType(keyType, valueType, _)  => if (row.isNullAt(index)) """\N""" else {
                    val mapInstance = row.getMap[Any, Any](index)
                    mapInstance.map(pair => {
                        val (key, value) = pair
                        row2string(Row(key), Array((keyType, 0)), separator + 2) +
                        (separator + 2).toChar.toString +
                        row2string(Row(value), Array((valueType, 0)), separator + 2)
                    }).mkString((separator + 1).toChar.toString)
                }
                case StructType(rowType)     => if (row.isNullAt(index)) """\N""" else {
                    val rowSchema = rowType.map(_.dataType).zipWithIndex
                    row2string(row.getStruct(index), rowSchema, separator + 1)
                }
            }
        })
        arr.mkString(separator.toChar.toString)
    }

    def writeHDFS(hqlContext: HiveContext, dstURL : String, hql : String) : DataFrame = {
        val (schema, fsAddr, dir, argument) = dstURL match {
            case ETL.hdfsURLExtractor(prefix, addr, dirStr, argumentStr) => (prefix, addr, dirStr, argumentStr)
        }
        val fs = if (fsAddr.trim.isEmpty) {
            hqlContext.sparkContext.hadoopConfiguration.get("fs.defaultFS")
        } else {
            schema + fsAddr.trim
        }
        val applicationId = hqlContext.sparkContext.applicationId
        val argumentList = argument.split("&").map(_.split("=")).map(arr => (arr(0), arr(1))).toMap
        val paths = getPaths(applicationId, fs, dir, argumentList)
        val concurrent = argumentList.getOrElse("concurrent", 2.toString).toInt
        val tableName = argumentList.getOrElse("table", "").trim
        val df = hqlContext.sql(hql)
        val dfSchema = df.schema.fields.map(_.dataType).zipWithIndex
        df.repartition(concurrent).foreachPartition(part => {
            val partitionIndex = TaskContext.get().partitionId()
            val fsURI = URI.create(fs)
            val hadoopConf = new Configuration()
            val bufferSize = 1024 * 1024
            val utf8Charset = Charset.forName("UTF-8")
            val bytes = ArrayBuffer.empty[Byte]
            var out : FSDataOutputStream = null
            try {
                val path = new Path(paths(partitionIndex))
                val filesystem  = FileSystem.get(fsURI, hadoopConf)
                if (!filesystem.exists(path)) { filesystem.create(path).close() }
                out = filesystem.append(path)
                while (part.hasNext) {
                    val row = part.next()
                    bytes ++= s"${row2string(row, dfSchema)}\n".getBytes(utf8Charset)
                    if (bytes.length >= bufferSize) {
                        out.write(bytes.toArray)
                        bytes.clear()
                    }
                }
                out.write(bytes.toArray)
            } catch {
                case e: Throwable =>
                    val pathStr = paths.mkString("[", ", ", "]")
                    logWarning(s"can not write into the No. $partitionIndex file in $pathStr :", e)
            } finally {
                if (out != null) { IOUtils.closeStream(out)}
                bytes.clear()
            }
        })
        val (newPaths, deltaPaths) = updatePaths(paths, applicationId, fs, dir, argumentList)
        if (tableName.nonEmpty && deltaPaths.nonEmpty) {
            deltaPaths.foreach(pair => {
                val (ymd, pathName) = pair
                val hiveLoadStatement =
                    s"""
                       |LOAD DATA INPATH '$fs$pathName' INTO TABLE $tableName PARTITION (stat_date = $ymd)
                     """.stripMargin
                hqlContext.sql(hiveLoadStatement)
            })
        }
        val dstDir = fs + dir + "?" + argument
        fsSinkPaths(dstDir) = newPaths
        hqlContext.emptyDataFrame
    }

    def executeHql(hqlContext: HiveContext, hql : String): DataFrame = {
        hql match {
            case jdbcInsertMatcher(tableName, jdbcURL, trNo1, trNo1Sql, trNo2, trNo2Sql, sql) =>
                writeJDBC(hqlContext, tableName, jdbcURL, trNo1, trNo1Sql, trNo2, trNo2Sql, sql.trim)
            case jdbcUpdateMatcher(tableName, updateSql, jdbcURL, callbackSQLStr, sql) =>
                updateJDBC(hqlContext, tableName, updateSql, jdbcURL, callbackSQLStr, sql.trim)
            case metaqMatcher(metaqURL, sql) => writeMetaQ(hqlContext, metaqURL, sql)
            case jsonMatcher(jsonTableName, sql) => createJsonTable(hqlContext, jsonTableName, sql.trim)
//            case esMatcher(esURL, sql)       => writeES(hqlContext, esURL, sql)
            case jdbcLoadMatcher(sparkTable, jdbcTable, jdbcURL) =>
                loadFromJDBC(hqlContext, sparkTable, jdbcTable, jdbcURL)
            case hdfsMatcher(hdfsURL, hqlStr) => writeHDFS(hqlContext, hdfsURL.trim, hqlStr.trim)
            case _ => hqlContext.sql(hql)
        }
    }

    /**
     * A workaround for bug specified by SPARK-10251 <a href = "https://issues.apache.org/jira/browse/SPARK-10251">
     * Some internal spark classes are not registered with kryo</a>
     */
    def registerExtraKryoClasses(conf : SparkConf)  = {
        val allClassNames = new LinkedHashSet[String]()
        val extraKryoClasses = Array(
            // Register types missed by Chill.
              None.getClass.getName
            , Nil.getClass.getName
            , "[Lscala.Tuple1;"
            , "[Lscala.Tuple2;"
            , "[Lscala.Tuple3;"
            , "[Lscala.Tuple4;"
            , "[Lscala.Tuple5;"
            , "[Lscala.Tuple6;"
            , "[Lscala.Tuple7;"
            , "[Lscala.Tuple8;"
            , "[Lscala.Tuple9;"
            , "[Lscala.Tuple10;"
            , "[Lscala.Tuple11;"
            , "[Lscala.Tuple12;"
            , "[Lscala.Tuple13;"
            , "[Lscala.Tuple14;"
            , "[Lscala.Tuple15;"
            , "[Lscala.Tuple16;"
            , "[Lscala.Tuple17;"
            , "[Lscala.Tuple18;"
            , "[Lscala.Tuple19;"
            , "[Lscala.Tuple20;"
            , "[Lscala.Tuple21;"
            , "[Lscala.Tuple22;"
            , "scala.collection.immutable.$colon$colon"
            , "scala.collection.mutable.WrappedArray$ofRef"
            , "scala.collection.immutable.Map$EmptyMap$"
            , classOf[ArrayBuffer[Any]].getName
            , classOf[Array[Any]].getName
            , classOf[scala.collection.immutable.Range].getName
            , classOf[java.util.concurrent.ConcurrentHashMap[_, _]].getName

            // Register types used by anystream
            , classOf[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema].getName
            , "org.apache.spark.sql.types.ByteType$"
            , "org.apache.spark.sql.types.ShortType$"
            , "org.apache.spark.sql.types.IntegerType$"
            , "org.apache.spark.sql.types.LongType$"
            , "org.apache.spark.sql.types.FloatType$"
            , "org.apache.spark.sql.types.DoubleType$"
            , "org.apache.spark.sql.types.DecimalType$"
            , "org.apache.spark.sql.types.StringType$"
            , "org.apache.spark.sql.types.BinaryType$"
            , "org.apache.spark.sql.types.BooleanType$"
            , "org.apache.spark.sql.types.TimestampType$"
            , "org.apache.spark.sql.types.DateType$"
            , "org.apache.spark.sql.types.ArrayType$"
            , "org.apache.spark.sql.types.MapType$"
            , classOf[org.apache.spark.sql.types.ArrayType].getName
            , classOf[org.apache.spark.sql.types.MapType].getName
            , classOf[org.apache.spark.sql.types.StructField].getName
            , classOf[org.apache.spark.sql.types.StructType].getName
            , classOf[org.apache.spark.sql.types.Metadata].getName
            , classOf[org.apache.spark.sql.types.UTF8String].getName
            , classOf[Array[org.apache.spark.sql.types.StructField]].getName
            , classOf[Array[org.apache.spark.streaming.receiver.Receiver[_]]].getName

            , classOf[com.taobao.metamorphosis.Message].getName
            , classOf[com.taobao.metamorphosis.cluster.Partition].getName

            , classOf[com.meizu.spark.streaming.metaq.MetaQReceiver].getName
            , classOf[com.meizu.spark.metaq.MetaQWriter].getName
            , classOf[Load].getName
        )
        allClassNames ++= conf.get("spark.kryo.classesToRegister", "").split(',').filter(_.nonEmpty)
        allClassNames ++= extraKryoClasses
        conf.set("spark.kryo.classesToRegister", allClassNames.mkString(","))
    }

    def createStreamingContext(checkpointDirectory: String,
                               transHqlPath: Option[String],
                               lowLatencyHqlPath: Option[String],
                               highLatencyHqlPath: Option[String]): StreamingContext = {
//        val appName  = getProperty("anystream.spark.appName", Some("AnySteam-ETL"))
        val streamingInterval = Math.abs(getProperty("anystream.spark.streaming.interval", None).toLong)
        val metaqZkConnect = getProperty("anystream.metaq.zkConnect", None)
        val metaqTopic  = getProperty("anystream.metaq.topic", None)
        val metaqGroup  = getProperty("anystream.metaq.group", None)
        val metaqRunner = getProperty("anystream.metaq.runners", Some(0.toString)).toInt
        val newMetaqParams = getProperty("anystream.metaq.params", Some("")).trim.split("&").map(setting => {
            val Array(key, value, _*) = setting.split("=").padTo(2, "")
            (key.trim, value.trim)
        }).toMap
        val metaqParams = if (metaqRunner > 0 ) {
            newMetaqParams + ("fetchRunnerCount" -> metaqRunner.toString)
        } else {
            newMetaqParams
        }
        logInfo("metaq configuration parameter setting: \n " + metaqParams.mkString("\n"))
//        val checkpointInterval = Math.abs(getProperty("anystream.spark.streaming.checkpointInterval", Some("5")).toInt)
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

        val sparkConf = new SparkConf()
        registerExtraKryoClasses(sparkConf)
        val ssc = new StreamingContext(sparkConf, Seconds(streamingInterval))

//        val messages = ssc.receiverStream(new MetaQReceiver(metaqZkConnect, metaqTopic, metaqGroup, metaqParams))
        val numReceivers = metaqParams.getOrElse("fetchRunnerCount", "2").toInt
        val metaqStreams = (1 to numReceivers).map(i => {
            ssc.receiverStream(new MetaQReceiver(metaqZkConnect, metaqTopic, metaqGroup, metaqParams))
        })
        val messages = ssc.union(metaqStreams)
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
                val bytes = compressionType match {
                    case 0 => load.getData.toByteArray
                    case 1 => Compression.uncompress[GZIPInputStream](load.getData.toByteArray)
                    case _ =>
                        logError("unknown data compression format "
                                + s"(msgId : ${msg.getId.toString}, msgInterface : ${load.getInterface})" )
                        Array.emptyByteArray
                }
                val data = magic match {
                    case 0 =>
                        new String(bytes, charset).split("\n").map(str => {
//                            str.replace(0x01.toChar, 0x11.toChar)
//                               .replace(0x02.toChar, 0x12.toChar)
//                               .replace(0x03.toChar, 0x13.toChar)
//                               .replace(0x04.toChar, 0x14.toChar)
//                               .replace(0x05.toChar, 0x15.toChar)
//                               .replace(0x06.toChar, 0x16.toChar)
//                               .replace(0x07.toChar, 0x17.toChar)
//                               .getBytes(charset)

                            str.map(chr => {
                                chr.toInt match {
                                    case 0x01 => 0x11.toChar
                                    case 0x02 => 0x12.toChar
                                    case 0x03 => 0x13.toChar
                                    case 0x04 => 0x14.toChar
                                    case 0x05 => 0x15.toChar
                                    case 0x06 => 0x16.toChar
                                    case 0x07 => 0x17.toChar
                                    case others => others.toChar
                                }
                            }).getBytes(charset)
                        })
                    case 1 =>
                        new String(bytes, charset).split("\n").map(str => {
                            str.getBytes(charset)
                        })
                    case _ =>
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
        
        val base = asDFStream.transform((rdd, time) => {
            val hqlContext = getInstance(rdd.sparkContext)
            hqlContext.clearCache()
            import hqlContext.implicits._

            var result : DataFrame = null
//            val df = rdd.toDF().withColumn("batch_timestamp", new Column(Literal(time.milliseconds)))
            val df = rdd.map(load => {
                val batchTimestamp = "batch_timestamp" -> time.milliseconds.toString
                load.copy(ext_domain = load.ext_domain + batchTimestamp)
            }).toDF()

            df.registerTempTable("__root__")
            for ((tableName, hql) <- transHqls if !hql.trim.toLowerCase.startsWith("insert")) {
                val tblDF = executeHql(hqlContext, hql) // hqlContext.sql(hql)
                if (tableName != "_") {
                    val npartions = Math.abs(hqlContext.getConf("anystream.dataframe.partitions", "0").toInt)
                    val isCached = hqlContext.getConf("anystream.dataframe.cache", "false").toBoolean
                    if (npartions == 0) {
                        tblDF.registerTempTable(tableName)
                    } else {
                        tblDF.coalesce(npartions).registerTempTable(tableName)
                    }
                    if (isCached) {
                        hqlContext.cacheTable(tableName)
                        hqlContext.setConf("anystream.dataframe.cache", "false")
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
                                val isCached = hqlContext.getConf("anystream.dataframe.cache", "false").toBoolean
                                if (npartions == 0) {
                                    tblDF.registerTempTable(tableName)
                                } else {
                                    tblDF.coalesce(npartions).registerTempTable(tableName)
                                }
                                if (isCached) {
                                    hqlContext.cacheTable(tableName)
                                    hqlContext.setConf("anystream.dataframe.cache", "false")
                                }
                            }
                        }
                    })
                case None =>
            }
        }

        val statOutputUrl   = getProperty("anystream.stat.output.url", Some("")).trim
        val statOutputTable = getProperty("anystream.stat.output.table", Some("T_ANYSTREAM_STAT")).trim
        val statInterfaces  = getProperty("anystream.stat.interfaces", Some("")).trim
        if (!statOutputUrl.trim.isEmpty) {
            val statInterfacesList = statInterfaces.trim.split(',').filterNot(_.trim.isEmpty)
            val interfacesFilterSql = if (statInterfacesList.isEmpty) {
                "SELECT * FROM `__stat__`"
            } else {
                val predicate = statInterfacesList.map("'" + _ + "'"). mkString("(", ", ", ")")
                s"SELECT * FROM `__stat__` WHERE interface IN $predicate"
            }
            val dataExpansionSql =
                """
                  | SELECT  interface
                  |       , split(coalesce(ext_domain['ip'], 'Unknown'), ':')       AS host
                  |       , array_bytes(data) + if(isnotnull(data), size(data), 0)  AS sendbyte
                  |       , if(isnotnull(data), size(data), 0)                      AS linenum
                  |       , send_timestamp
                  | FROM `__stat_input__`
                """.stripMargin
            val statAnalysisSql =
                s"""
                  | SELECT  cast(NULL AS INT) AS id
                  |       , 'merger'          AS vendor
                  |       , '$metaqTopic'     AS topic
                  |       , interface
                  |       , hostip
                  |       , sum(sendbyte)     AS sendbyte
                  |       , sum(linenum)      AS linenum
                  |       , sendtime
                  |       , unix_timestamp()  AS inserttime
                  | FROM ( SELECT   interface
                  |               , concat_ws(':', host[1], host[2]) AS hostip
                  |               , sendbyte
                  |               , linenum
                  |               , partitioner('m', send_timestamp) AS sendtime
                  |        FROM `__stat_data__` ) tmp
                  | GROUP BY interface, hostip, sendtime
                """.stripMargin
            val statWindow = Seconds(Math.ceil(60.0 / streamingInterval).toInt * streamingInterval)
            asDFStream.window(statWindow, statWindow).foreachRDD(rdd => {
                val hqlContext = getInstance(rdd.sparkContext)
                import hqlContext.implicits._
                rdd.toDF().registerTempTable("__stat__")
                val stat_input = hqlContext.sql(interfacesFilterSql)
                stat_input.registerTempTable("__stat_input__")
                val stat_data = hqlContext.sql(dataExpansionSql)
                stat_data.registerTempTable("__stat_data__")
                val statResult = hqlContext.sql(statAnalysisSql)
                try {
                    statResult.coalesce(8).write.mode(SaveMode.Append).jdbc(statOutputUrl, statOutputTable, new Properties)
                } catch {
                    case e: Exception => logWarning("fail to ouput satistical information : " + e.getStackTraceString)
                }
            })
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
//        val hqlContext = getInstance(ssc.sparkContext)
//        HiveThriftServer2.startWithContext(hqlContext)
        ssc.start()
        ssc.awaitTermination()
    }
}
