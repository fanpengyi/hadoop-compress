package com.people.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * 本例使用框架默认的Reducer，它将Mapper输入的kv对，原样输出；所以reduce输出的kv类型分别是Text, NullWritable
 * 自定义OutputFormat的类，泛型表示reduce输出的键值对类型；要保持一致;
 * map--(kv)-->reduce--(kv)-->OutputFormat
 */
public class MyOutPutFormat extends FileOutputFormat<Text, NullWritable> {

    /**
     * 两个输出文件;
     * good用于保存好评文件；其它评级保存到bad中
     * 根据实际情况修改path;node01及端口号8020
     */
    String bad = "hdfs://node01:8020/outputformat/bad/r.txt";
    String good = "hdfs://node01:8020/outputformat/good/r.txt";

    /**
     *
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        //获得文件系统对象
        FileSystem fs = FileSystem.get(context.getConfiguration());
        //两个输出文件路径
        Path badPath = new Path(bad);
        Path goodPath = new Path(good);
        FSDataOutputStream badOut = fs.create(badPath);
        FSDataOutputStream goodOut = fs.create(goodPath);
        return new MyRecordWriter(badOut,goodOut);
    }

    /**
     * 泛型表示reduce输出的键值对类型；要保持一致
     */
    static class MyRecordWriter extends RecordWriter<Text, NullWritable>{

        FSDataOutputStream badOut = null;
        FSDataOutputStream goodOut = null;

        public MyRecordWriter(FSDataOutputStream badOut, FSDataOutputStream goodOut) {
            this.badOut = badOut;
            this.goodOut = goodOut;
        }

        /**
         * 自定义输出kv对逻辑
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if (key.toString().split("\t")[9].equals("0")){//好评
                goodOut.write(key.toString().getBytes());
                goodOut.write("\r\n".getBytes());
            }else{//其它评级
                badOut.write(key.toString().getBytes());
                badOut.write("\r\n".getBytes());
            }
        }

        /**
         * 关闭流
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(goodOut !=null){
                goodOut.close();
            }
            if(badOut !=null){
                badOut.close();
            }
        }
    }
}
