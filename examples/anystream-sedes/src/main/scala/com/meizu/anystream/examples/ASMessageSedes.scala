package com.meizu.anystream.examples

import com.google.protobuf.ByteString
import com.meizu.anystream._
import scala.collection.JavaConverters._
import scala.collection.mutable.StringBuilder
/**
 * @author ${user.name}
 */
object ASMessageSedes {
  
    def ASMessageSerializer() : Array[Byte] = {
        ASMessage.ASDataFrame.newBuilder()
            .addExtDomain(ASMessage.ASMapEntity.newBuilder()
                .setKey("Key")
                .setValue("Value")
                .build())
            .setInterface("test.anystream_test")
            .setData(ByteString.copyFrom(("AnyStream" + 0x01.toChar + "Hello World").getBytes))
            .setConfigId(1)
            .build().toByteArray
    }
    def ASMessageDeserializer(bytes: Array[Byte]): ASMessage.ASDataFrame = {
        ASMessage.ASDataFrame.parseFrom(bytes)
    }

    def show(df: ASMessage.ASDataFrame): Unit = {
         println("interface : " + df.getInterface)
         val magic = if(df.hasMagic) df.getMagic.toString
             else df.getMagic.toString + " (default)"
         println("magic : " + magic )
         val partition = if(df.hasPartition) df.getPartition
             else df.getPartition.toString + " (default)"
         println("partition : " + partition )
         val data = if (df.hasData)
             new String(df.getData.toByteArray).split(0x01.toChar).reduce((a, b) => a + " # " + b)
         else "No Data"
         println("data : " + data)
         val send_timestamp = if (df.hasSendTimestamp) df.getSendTimestamp.toString
             else df.getSendTimestamp.toString + " (default)"
         println("send_timestamp : " + send_timestamp)
         val extDomainList = df.getExtDomainList.asScala
         println(extDomainList.mkString("extDomain : ", " , ", ""))
         val configID = if (df.hasConfigId) df.getConfigId.toString
             else df.getConfigId.toString + " (default)"
         println("configID : " + configID)
    }
  
    def main(args : Array[String]) {
        val data = ASMessageSerializer()
        val msg = ASMessageDeserializer(data)
        show(msg)
    }

}
