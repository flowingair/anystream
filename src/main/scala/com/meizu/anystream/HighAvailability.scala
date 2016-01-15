package com.meizu.anystream

import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.I0Itec.zkclient.{ZkClient, IZkDataListener, IZkStateListener, ContentWatcher}
import org.apache.zookeeper.Watcher.Event.KeeperState

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock


class HighAvailability(ssc : StreamingContext, thriftServerURL : String) extends Logging {
    private def getProperty(key: String, default: Option[String]): String = {
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

    private val stopFlag = new AtomicBoolean(false)
    private val znodeData = thriftServerURL

    def start() : Unit = {
        val appName = ssc.sparkContext.appName
        val appId   = ssc.sparkContext.applicationId
        val haZkZnodeParent = getProperty("anystream.ha.znode.parent", Some("/anystream/spark"))
        val asRunningZnode  = s"$haZkZnodeParent/$appName%$appId%running"
        val asStoppingZnode = s"$haZkZnodeParent/$appName%$appId%stopping"
        val haZkConnect = getProperty("anystream.ha.zkConnect", Some(""))

        if (haZkConnect.trim.isEmpty) { return }

        val zkClient = new ZkClient(haZkConnect)
        val lock = new ReentrantLock()
        val condition = lock.newCondition()

        val sessionStateListener = new IZkStateListener {
            override def handleNewSession(): Unit = {
                logInfo("New Session created")
                val asZkPath = if (stopFlag.get) asStoppingZnode else asRunningZnode
                zkClient.createEphemeral(asZkPath, znodeData)
                logInfo(s"Recreate Ephemeral node '$asZkPath' with data '$znodeData'")

            }

//            override def handleSessionEstablishmentError(throwable: Throwable): Unit = {
//                logInfo(s"Session Establishment Error : ${throwable.getStackTraceString}")
//            }

            override def handleStateChanged(keeperState: KeeperState): Unit = {
                logInfo(s"Session State Changed : $keeperState")
            }
        }

        val asStoppingPathDataListener = new IZkDataListener {
            override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
                logInfo(s"Data Changed : '$dataPath' with Data '${data.asInstanceOf[String]}'")
            }
            override def handleDataDeleted(dataPath: String): Unit = {
                logInfo(s"Data Deleted : '$dataPath'")
                zkClient.createEphemeral(dataPath, znodeData)
                logInfo(s"Recreate Ephemeral node '$dataPath' with data '$znodeData'")
            }
        }

        val asRunningPathDataListener = new IZkDataListener {
            override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
                logInfo(s"Data Changed : '$dataPath' with Data '${data.asInstanceOf[String]}'")
            }

            override def handleDataDeleted(dataPath: String): Unit = {
                logInfo(s"Data Deleted : '$dataPath'")

                zkClient.unsubscribeDataChanges(dataPath, this)

                try {
                    zkClient.subscribeDataChanges(asStoppingZnode, asStoppingPathDataListener)
                    zkClient.createEphemeral(asStoppingZnode, znodeData)
                    logInfo(s"Create Ephemeral node '$asStoppingZnode' with data '$znodeData'")
                } finally {
                    lock.lock()
                    try {
                        stopFlag.set(true)
                        condition.signalAll()
                    } finally {
                        lock.unlock()
                    }
                }
            }
        }

        val stopThread = new Thread {
            override def run() : Unit = {
                logInfo("Start spark streaming monitor")
                lock.lock()
                try {
                    while (!stopFlag.get) {
                        logInfo("awaiting for stopping spark streaming")
                        condition.await()
                    }
                } finally {
                    lock.unlock()
                }
                val sc = ssc.sparkContext
                log.info("Start stopping gracefully Spark Streaming!")
                ssc.stop(stopSparkContext = false, stopGracefully = true)
                log.info("Spark Stream has gracefully stopped")
                zkClient.unsubscribeAll()
                zkClient.delete(asStoppingZnode)
                zkClient.close()
                sc.stop()
            }
        }

        zkClient.subscribeStateChanges(sessionStateListener)
        zkClient.subscribeDataChanges(asRunningZnode, asRunningPathDataListener)
        zkClient.delete(asRunningZnode)
        zkClient.createEphemeral(asRunningZnode, znodeData)
        stopThread.setName("Spark streaming Monitor")
        stopThread.start()
    }
}