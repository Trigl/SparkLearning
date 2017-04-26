package com.trigl.spark.util

import java.io.DataOutputStream

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter, TextOutputFormat}
import org.apache.hadoop.util.{Progressable, ReflectionUtils}

/**
  *
  * Created by 白鑫 on 2017-04-11 9:58
  */
class RDDTextOutputFormat[K,V] extends TextOutputFormat[K,V] {

  override def getRecordWriter(ignored: FileSystem, job: JobConf, name: String, progress: Progressable): RecordWriter[K, V] = {
    val isCompressed: Boolean = FileOutputFormat.getCompressOutput(job)
    val keyValueSeparator: String = job.get("mapreduce.output.textoutputformat.separator", "\t")
//    val iname = name + System.currentTimeMillis()
    if (!isCompressed) {
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile : Path = new Path(FileOutputFormat.getOutputPath(job), name)
      val fileOut : FSDataOutputStream = if (fs.exists(newFile)) {
        fs.append(newFile)
      } else {
        fs.create(file, progress)
      }
      new TextOutputFormat.LineRecordWriter[K, V](fileOut, keyValueSeparator)
    } else {
      val codecClass: Class[_ <: CompressionCodec] = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
      // create the named codec
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, job)
      // build the filename including the extension
      val file: Path = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(job)
      val newFile : Path = new Path(FileOutputFormat.getOutputPath(job), name + codec.getDefaultExtension)

      val fileOut: FSDataOutputStream = if (fs.exists(newFile)) {
        fs.append(newFile)
      } else {
        fs.create(file, progress)
      }
      new TextOutputFormat.LineRecordWriter[K, V](new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator)
    }
  }
}
