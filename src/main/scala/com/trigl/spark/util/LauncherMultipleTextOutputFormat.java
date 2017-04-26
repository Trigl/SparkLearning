package com.trigl.spark.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

/**
 * Created by Trigl on 2017/04/15.
 */
public class LauncherMultipleTextOutputFormat<K, V> extends MultipleTextOutputFormat<K, V> {

    private TextOutputFormat<K, V> theTextOutputFormat = null;

    public RecordWriter getRecordWriter(final FileSystem fs, final JobConf job, final String name, final Progressable arg3) throws IOException {

        return new RecordWriter() {

            TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap();

            public void write(Object key, Object value) throws IOException {

                // 完整文件路径名
//                String dir = "/changmi/old/arrival_data/" + key.toString() + name.substring(name.length() - 4);
                String dir = "/changmi/launcher/click/" + key.toString() + name;
                // 内容
                String line = value.toString();

                RecordWriter rw = (RecordWriter) this.recordWriters.get(dir);

                try {
                    if (rw == null) {
                        rw = getBaseRecordWriter(fs, job, dir, arg3);
                        this.recordWriters.put(dir, rw);
                    }
                    rw.write(line, null);

                } catch (Exception e) {
                    //一个周期内，job不能完成，下一个job启动，会造成同时写一个文件的情况，变更文件名，添加后缀
                    this.rewrite(dir + "-", line);
                }

            }

            public void rewrite(String path, String line) {

                String finalPath = path + new Random().nextInt(10);
                RecordWriter rw = (RecordWriter) this.recordWriters.get(finalPath);
                try {
                    if (rw == null) {
                        rw = getBaseRecordWriter(fs, job, finalPath, arg3);
                        this.recordWriters.put(finalPath, rw);
                    }
                    rw.write(line, null);
                } catch (Exception e) {
                    //重试
                    this.rewrite(finalPath, line);
                }
            }

            public void close(Reporter reporter) throws IOException {

                Iterator keys = this.recordWriters.keySet().iterator();
                while (keys.hasNext()) {
                    RecordWriter rw = (RecordWriter) this.recordWriters.get(keys.next());
                    rw.close(reporter);
                }
                this.recordWriters.clear();
            }
        };
    }


    protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs, JobConf job, String path, Progressable arg3) throws IOException {
        if (this.theTextOutputFormat == null) {
            this.theTextOutputFormat = new RDDTextOutputFormat();
        }
        return this.theTextOutputFormat.getRecordWriter(fs, job, path, arg3);
    }
}