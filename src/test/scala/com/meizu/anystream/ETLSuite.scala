package com.meizu.anystream

import org.scalatest.FunSuite
import org.scalatest.Assertions
import org.scalatest.matchers.ShouldMatchers
import org.rogach.scallop.{ScallopOption, ScallopConf}
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.sql.hive.HiveContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
//import org.apache.spark.sql.hive.test._

@RunWith(classOf[JUnitRunner])
class ETLSuite extends FunSuite with ShouldMatchers{

    ignore("parse hql file") {
        val root = System.getProperty("user.dir")
        ETL.isDebugMode = true
        val hqls = ETL.parseHql(root + "/src/test/scala/as_sm_ll_action.hive.sql")
        hqls.foreach(println)
    }

    test("a jdbcLoadMatcher should extract clause of loading data from external DB via JDBC interface") {
        val str =
            """
              | IMPORT TABLE imei_whitelist FROM t_uxip_test_imei_white
              | USING 'jdbc:mysql://172.16.10.235:3306/MEIZU_DB_ORION?user=mysqluser&password=mysqluser'
            """.stripMargin
        val (sparkTable, jdbcTable, jdbcURL) = str match {
            case ETL.jdbcLoadMatcher(st, et, url) => (st, et, url)
            case _=> ("", "", "")
        }
//        println(sparkTable)
//        println(jdbcTable)
//        println(jdbcURL)

        sparkTable should be ("imei_whitelist")
        jdbcTable should be ("t_uxip_test_imei_white")
        jdbcURL should be ("jdbc:mysql://172.16.10.235:3306/MEIZU_DB_ORION?user=mysqluser&password=mysqluser")
    }

    test("a mergeMatcher should extract clause of merging into destination directory from source directory") {
        val str =
        """MERGE INTO DIRECTORY '/user/hive/tmp/as_uxip' FROM '/user/hive/warehouse/anystream_uxip.db/bdl_fdt_uxip_events'"""
        val (dest, src) = str match {
            case ETL.mergeMatcher(dir1, dir2) => (dir1, dir2)
            case _=> (" ", " ")
        }
        println(dest)
        println(src)
        dest should be ("/user/hive/tmp/as_uxip")
        src  should be ("/user/hive/warehouse/anystream_uxip.db/bdl_fdt_uxip_events")
    }

    test("a jsonMatcher should extract clause of creating jsonTable from stringTable") {
        val str =
            """
              | CREATE TEMPORARY TABLE jsonTable
              | USING org.apache.spark.sql.json
              | AS
              | SELECT '{"k1":"v1", "k2":"v2", "k3":{"sk1":"sv1", "sk2":"sv2"}}'
            """.stripMargin
        val (tableName, hql) = str match {
            case ETL.jsonMatcher(table, hqlClause) => (table, hqlClause.trim)
            case _ => ("", "")
        }
//        println(str)
//        println(hql)
        tableName should be ("jsonTable")
        hql should be ("""SELECT '{"k1":"v1", "k2":"v2", "k3":{"sk1":"sv1", "sk2":"sv2"}}'""")
    }

    test("a jdbcInsertMatcher should be extract clause of insert into external tables from spark table"){
        val str =
            """
              | INSERT INTO TABLE tableName (x, y, z)
              | USING 'jdbc:sqlserver://ip:port/DatabaseName=xxx\;user=meizu\;password=12345'
              | SQL on ENTER 'delete * from xxx\;'
              | SQL on EXIT 'drop table yyy\; insert into zzz(x) values (\'abc\')'
              | SELECT * FROM sparkTable
            """.stripMargin

        val (tableName, jdbcUrl, trNo1, trNo1Sql, trNo2, trNo2Sql, hql) = str match {
            case ETL.jdbcInsertMatcher(table, url, tr1, tr1Sql, tr2, tr2Sql, sql) =>
                (table, url, tr1, tr1Sql, tr2, tr2Sql, sql.trim)
            case _ => ("", "", "", "", "", "", "")
        }
        println(tableName)
        println(jdbcUrl.replaceAll("""\\;""", """;"""))
        println(trNo1)
        println(trNo1Sql)
        println(trNo2)
        println(trNo2Sql)
        println(hql)
        tableName should be ("tableName (x, y, z)")
        jdbcUrl.replaceAll("""\\;""", """;""") should be ("jdbc:sqlserver://ip:port/DatabaseName=xxx;user=meizu;password=12345")
        trNo1 should be ("ENTER")
        trNo1Sql should be ("""delete * from xxx\;""")
        trNo2 should  be ("EXIT")
        trNo2Sql should be ("""drop table yyy\; insert into zzz(x) values (\'abc\')""")
        hql should be ("SELECT * FROM sparkTable")
    }

    test("a jdbcUpadteMatcher should extract clause of updating external tables " +
         "with data in spark-sql via jdbc interface, and extract callback when callback exists") {
        val str =
            """
              | UPDATE TABLE tableName SET col1 = %1, col2 = %3 WHERE col3 in (%3)
              | USING 'jdbc:mysql://ip:host/db?user=orion&password=12345'
              | CALLBACK ON NOUPDAtE 'insert into ddd (x) values (\'xxx\')'
              | SELECT * FROM sparkTable
            """.stripMargin
        val (tableName, setClause, jdbcUrl, callback, hql) = str match {
            case ETL.jdbcUpdateMatcher(table, setStr, url, cb, sql) => (table, setStr.trim, url, cb, sql.trim)
            case _ => ("", "", "", "")
        }

        println(tableName)
        println(setClause)
        println(jdbcUrl)
        println(hql)
        println(callback)
        tableName should be ("tableName")
        jdbcUrl should be ("jdbc:mysql://ip:host/db?user=orion&password=12345")
        setClause should be ("col1 = %1, col2 = %3 WHERE col3 in (%3)")
        hql should be ("SELECT * FROM sparkTable")
        callback should be ("""insert into ddd (x) values (\'xxx\')""")
    }

    test("a jdbcUpadteMatcher should extract clause of updating external tables " +
         "with data in spark-sql via jdbc interface") {
        val str =
            """
              | UPDATE TABLE tableName SET col1 = %1, col2 = %3 WHERE col3 in (%3)
              | USING 'jdbc:mysql://ip:host/db?user=orion&password=12345'
              | SELECT * FROM sparkTable
            """.stripMargin
        val (tableName, setClause, jdbcUrl, callback, hql) = str match {
            case ETL.jdbcUpdateMatcher(table, setStr, url, cb, sql) => (table, setStr.trim, url, cb, sql.trim)
            case _ => ("", "", "", "")
        }
//        println(tableName)
//        println(setClause)
//        println(jdbcUrl)
//        println(hql)
//        println(callback)
        tableName should be ("tableName")
        jdbcUrl should be ("jdbc:mysql://ip:host/db?user=orion&password=12345")
        setClause should be ("col1 = %1, col2 = %3 WHERE col3 in (%3)")
        hql should be ("SELECT * FROM sparkTable")
        callback should equal (null)
    }

    test("a jdbcUpdateSqlTransformer shoulb repalce matched string with given string") {
        val str =
            """
              |SET col1 = %1, col2 = %3, col%%4 = %5 WHERE col3 in (%3)
            """.stripMargin
        val replacedStr = str.replaceAll("""((?<!%)%\d+)""", """\?""") //.replaceAll("%%", "%")
//        val result = replacedStr.format(List("A", "B", "C", "D", "E"): _*)
        val result = replacedStr.format("")
        println(replacedStr)
        println(result)
    }

    test("Get hbase.zookeeper.quorum from jdbc url required by apache phoenix"){
        val str = "jdbc:phoenix:zkHost1,zkHost2,zkHost3:2181:/hbase"
        val phoenix = str.trim.split(""":""")
        println(phoenix(2) + ":" + phoenix(3))
    }

    test("test scallop toggle") {
        val toggle1  = new ArgsOptsConf(List("--debug", "--conf", "xxx"))
        toggle1.debugMode.get should equal(Some(true))
        toggle1.debugMode.isSupplied should equal(true)

        val toggle2  = new ArgsOptsConf(List("--conf", "xxx"))
        toggle2.debugMode.get should equal(Some(false))
        toggle2.debugMode.isSupplied should equal(false)

        val toggle3  = new ArgsOptsConf(List("--nodebug", "--conf", "xxx"))
        toggle3.debugMode.get should equal(Some(false))
        toggle3.debugMode.isSupplied should equal(true)
    }

    ignore("test updateJDBC") {
        if (System.getProperty("os.name").toLowerCase.contains("windows")) {
            System.setProperty( """hadoop.home.dir""", """D:\hadoop-2.2.0""")
        }
        val hqlCtxt = new HiveContext(
            new SparkContext("local[2]", "ANYSTREAM-FRAMEWORK-TEST",
                new SparkConf()
                //                .set("spark.sql.test", "")
                //                .set("spark.sql.hive.metastore.barrierPrefixes", "org.apache.spark.sql.hive.execution.PairSerDe")
            )
        )
        val hqlEx = Array(
            """CREATE TEMPORARY TABLE ServiceMetric
              |USING org.apache.spark.sql.json
              |AS
              |SELECT '{"state" : [{"id":"0", "ap":"ap", "k1":"k1", "k2":"k2", "k3":"1270010010010", "vs":[0, 0, 0, 0, 0, 0]}], "ip":"127001001001", "t":"0"}'
              |""".stripMargin ,
            """SELECT  stat.ap as appName
              |      , stat.k1 as service
              |      , stat.k2 as method
              |      , substr(stat.k3, 1, 12) as rip
              |      , stat.vs[0] AS counts
              |      , stat.vs[1] AS ok_counts
              |      , stat.vs[2] AS timeout_counts
              |      , stat.vs[3] AS reject_counts
              |      , stat.vs[4] AS err_counts
              |      , stat.vs[5] AS rt
              |      , cast(ip AS STRING) AS ip
              |      , cast(t  AS BIGINT)  AS t
              |      , cast(substr(stat.k3, 13, 1) AS BIGINT) as type
              |      , cast(from_unixtime(CAST(t / 1000 AS BIGINT),'yyyyMMdd') AS BIGINT) nowtime
              |FROM ServiceMetric LATERAL VIEW explode(state) stateTable AS stat
              |""".stripMargin ,
            """UPDATE TABLE T_SERVER_IND_METRIC SET
              |( ROWKEY
              |, SER_IND_MTC.APPNAME
              |, SER_IND_MTC.RIP
              |, SER_IND_MTC.COUNT
              |, SER_IND_MTC.SCOUNT
              |, SER_IND_MTC.RCOUNT
              |, SER_IND_MTC.ECOUNT
              |, SER_IND_MTC.TOCOUNT
              |, SER_IND_MTC.RT
              |, SER_IND_MTC.TIME
              |, SER_IND_MTC.TYPE)
              |SELECT
              |  %1
              |, %2
              |, %3
              |, TO_CHAR(TO_NUMBER(SER_IND_MTC.COUNT) + %4)
              |, TO_CHAR(TO_NUMBER(SER_IND_MTC.SCOUNT) + %5)
              |, TO_CHAR(TO_NUMBER(SER_IND_MTC.RCOUNT) + %6)
              |, TO_CHAR(TO_NUMBER(SER_IND_MTC.ECOUNT) + %7)
              |, TO_CHAR(TO_NUMBER(SER_IND_MTC.TOCOUNT) + %8)
              |, TO_CHAR(TO_NUMBER(SER_IND_MTC.RT) + %9)
              |, TO_CHAR(%10)
              |, TO_CHAR(%11)
              |FROM T_SERVER_IND_METRIC WHERE ROWKEY = %1
              |USING 'jdbc:phoenix:172.16.200.239,172.16.200.233,172.16.200.234:2181'
              |CALLBACK ON NOUPDATE 'UPSERT INTO T_SERVER_IND_METRIC(
              |  ROWKEY
              |, SER_IND_MTC.APPNAME
              |, SER_IND_MTC.RIP
              |, SER_IND_MTC.COUNT
              |, SER_IND_MTC.SCOUNT
              |, SER_IND_MTC.RCOUNT
              |, SER_IND_MTC.ECOUNT
              |, SER_IND_MTC.TOCOUNT
              |, SER_IND_MTC.RT
              |, SER_IND_MTC.TIME
              |, SER_IND_MTC.TYPE)
              |VALUES(%1, %2, %3, %4, %5, %6, %7, %8, %9, %10, %11 )'
              |SELECT  concat_ws('-', cast(rip AS STRING), cast(type AS STRING), cast(t AS STRING))
              |      , appName
              |      , rip
              |      , cast(counts AS BIGINT)
              |      , cast(ok_counts AS BIGINT)
              |      , cast(reject_counts AS BIGINT)
              |      , cast(err_counts  AS BIGINT)
              |      , cast(timeout_counts AS BIGINT)
              |      , cast(rt AS BIGINT)
              |      , cast(t AS BIGINT)
              |      , cast(type AS BIGINT)
              |FROM StatesDetail
              |""".stripMargin
        )
        ETL.executeHql(hqlCtxt, hqlEx(0))
        ETL.executeHql(hqlCtxt, hqlEx(1)).registerTempTable("StatesDetail")
        ETL.executeHql(hqlCtxt, hqlEx(2))
    }
}