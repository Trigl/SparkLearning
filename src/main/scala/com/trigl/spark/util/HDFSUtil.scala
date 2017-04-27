package com.trigl.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  *
  * @author 白鑫
  */
object HDFSUtil {
  val HDFS_PATH = "hdfs://ns"


  def delete(path: String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", HDFS_PATH)
    val fs = FileSystem.get(conf)
    deleteEmptyFile(fs, path)
    fs.close()
  }

  def deleteEmptyFile(fs: FileSystem, path: String): Unit = {

    val srcPath = new Path(path)
    val fileStatus = fs.listStatus(srcPath)
    for (file <- fileStatus) {
      if (file.isFile()) {
        // 空文件删除
        if (file.getLen() == 0) {
          fs.delete(file.getPath(), true)
        }
      } else {
        deleteEmptyFile(fs, file.getPath().toString())
      }

    }

  }


}
