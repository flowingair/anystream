package com.meizu.anystream

import org.scalatest.FunSuite
import org.scalatest.Assertions
import org.scalatest.matchers.ShouldMatchers
import org.rogach.scallop.{ScallopOption, ScallopConf}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ETLSuite extends FunSuite with ShouldMatchers{
//    test("parse hql file") {
//        val root = System.getProperty("user.dir")
//        ETL.isDebugMode = true
//        val hqls = ETL.parseHql(root + "/src/test/scala/test.hive.sql")
//        println (hqls)
//    }

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

    test("a jdbcInsertMatcher should be extract clause of insert into external tables " +
         "with data in spark-sql via jdbc interface"){
        val str =
            """
              | INSERT INTO TABLE tableName
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
        tableName should be ("tableName")
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

//        println(tableName)
//        println(setClause)
//        println(jdbcUrl)
//        println(hql)
//        println(callback)
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
        val replacedStr = str.replaceAll("""((?<!%)%\d+)""", """$1\$s""") //.replaceAll("%%", "%")
        val result = replacedStr.format(List("A", "B", "C", "D", "E"): _*)
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
}