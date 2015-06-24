package com.meizu.anystream.util

import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import java.io.{IOException, InputStream, OutputStream, ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.mutable
import scala.reflect._
import org.apache.spark.Logging

/**
 * @author ${user.name}
 */
object Compression extends Logging {

    def compress[Out <: OutputStream](bytes : Array[Byte])(implicit m: ClassTag[Out]): Array[Byte] = {
        var bytesOut: ByteArrayOutputStream = null
        var compressOut : Out = null.asInstanceOf[Out]
        val data = mutable.ArrayBuilder.make[Byte]
        try {
            bytesOut = new ByteArrayOutputStream()
            compressOut = m.runtimeClass
                .getConstructor(classOf[OutputStream])
                .newInstance(bytesOut)
                .asInstanceOf[Out]
            compressOut.write(bytes)
            compressOut.flush()
            compressOut.close()
            data ++= bytesOut.toByteArray
        } catch {
            case e: IOException => logError("Compress data exception : " + e.getStackTraceString)
        } finally {
            if (compressOut != null) compressOut.close()
            if (bytesOut != null) bytesOut.close()
        }
        data.result()
    }

    def uncompress[In <: InputStream](bytes : Array[Byte])(implicit m: ClassTag[In]): Array[Byte] = {
        var bytesIn: ByteArrayInputStream = null
        var uncompressIn : In = null.asInstanceOf[In]
        val buffer = new Array[Byte](8192)
        val data = mutable.ArrayBuilder.make[Byte]
        try {
            bytesIn = new ByteArrayInputStream(bytes)
            uncompressIn = m.runtimeClass
                .getConstructor(classOf[InputStream])
                .newInstance(bytesIn)
                .asInstanceOf[In]
            var length = uncompressIn.read(buffer)
            while (length > 0) {
                data ++= buffer.take(length)
                length = uncompressIn.read(buffer)
            }
        } catch {
            case e: IOException => logError("UnCompress data exception : " + e.getStackTraceString)
        }finally {
            if (uncompressIn != null) uncompressIn.close()
            if (bytesIn != null) bytesIn.close()
        }
        data.result()
    }
}

// object App {
//     private final val utf8Charset = Charset.forName("UTF-8")



//     def main(args : Array[String]): Unit = {
// //        val className = ""
// //        val classInstance = Class.forName(className)
// //        val obj = classInstance.newInstance()

//         val str = new String("Hello World, 中华人民共和国".getBytes(utf8Charset), utf8Charset)
//         val compressBytes = CompressionUtil.compress[GZIPOutputStream](str.getBytes(utf8Charset))
//         val decompressBytes = CompressionUtil.uncompress[GZIPInputStream](compressBytes)
//         val result = new String(decompressBytes, utf8Charset)
//         println("result : " + result)
//     }
// }
