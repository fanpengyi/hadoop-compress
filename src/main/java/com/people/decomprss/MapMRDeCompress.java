package com.people.decomprss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * 用于将hadoop 数据压缩 参数输入路径 输出路径
 *
 * @author fanpengyi
 * @data 2019-11-13
 * @version 1.0
 *
 */
public class MapMRDeCompress {

    public static final String DECOMPRESS_NAME = ".txt";

    public static final String INPUT_PATH = "hdfs://hadoop102:9000/mycompress/year=2018/month=06/2018-05-24-16/2018-05-24-16.bz2";
    public static final String OUTPUT_PATH = "hdfs://hadoop102:9000/myDecompress/year=2018/month=06/";

    public static final boolean USE_OUT_PARAM = true;

    public static Logger logger = LoggerFactory.getLogger(MapMRDeCompress.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //判断以下，输入参数是否是两个，分别表示输入路径、输出路径

        if(USE_OUT_PARAM){
            args = new String[2];
            // args[0] = "hdfs://nserver:9000/data_center/crawl/new_article/year=2018/month=06/2018-06-01-00.txt";
            // args[0] = "hdfs://hadoop102:9000/big";
            args[0] = INPUT_PATH;
            //args[1] = "D:\\compress\\_5_bzip2";
            args[1] = OUTPUT_PATH;
        }


        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        String[] inputSplit = args[0].split("/");

        if(inputSplit == null || inputSplit.length == 0){
            logger.error("输入路径有误！");
            return;
        }

        String[] inputFileNameSplit = inputSplit[inputSplit.length - 1].split("\\.");

        if(inputFileNameSplit == null || inputFileNameSplit.length == 0){
            logger.error("输入路径有误,没有 带 .txt");
            return;
        }

        args[1] =  args[1] + "/" + inputFileNameSplit[0];


        Configuration configuration = new Configuration();

        //调用getInstance方法，生成job实例
        Job job = Job.getInstance(configuration, MapMRDeCompress.class.getSimpleName());
        //设置jar包，参数是包含main方法的类
        job.setJarByClass(MapMRDeCompress.class);


        //设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置处理Map阶段的自定义的类
        job.setMapperClass(SearchCountMapper.class);
        //设置map combine类，减少网路传出量
        //设置处理Reduce阶段的自定义的类
        job.setReducerClass(SearchCountReducer.class);

        //如果map、reduce的输出的kv对类型一致，直接设置reduce的输出的kv对就行；如果不一样，需要分别设置map, reduce的输出的kv类型
        //注意：此处设置的map输出的key/value类型，一定要与自定义map类输出的kv对类型一致；否则程序运行报错
        job.setOutputFormatClass(MyOutPutFormat.class);
        //job.setOutputFormatClass(FileOutputFormat.class);

        //设置reduce task最终输出key/value的类型
        //注意：此处设置的reduce输出的key/value类型，一定要与自定义reduce类输出的kv对类型一致；否则程序运行报错
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 提交作业
        job.waitForCompletion(true);

        logger.info(  "输入路径为："+args[0]+"    Decompress ---->   "+"输出路径"+args[1]);

        logger.info(  "    Decompress success!   ");

        TimeUnit.SECONDS.sleep(5);
    }

    public static class SearchCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        //定义共用的对象，减少GC压力
        Text userIdKOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获得当前行的数据
            //样例数据：20111230111645  169796ae819ae8b32668662bb99b6c2d        塘承高速公路规划线路图  1       1       http://auto.ifeng.com/roll/20111212/729164.shtml
            String line = value.toString();
            userIdKOut.set(line);
            //输出结果
            context.write(userIdKOut, NullWritable.get());
        }
    }


    public static class SearchCountReducer extends Reducer<Text, NullWritable, Text, NullWritable> {


        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }


    //建立一个类，继承TextOutputFormat，此class位于org.apache.hadoop.mapreduce.lib.output
    private static class MyOutPutFormat extends TextOutputFormat {

       /**
         *
         * @param context
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            //hdfs://hadoop102:9000/test/1-txt
//TODO  jiaoyan shuliang log
            Path[] inputPaths = FileInputFormat.getInputPaths(context);
            Path outputPath = FileOutputFormat.getOutputPath(context);

            if(inputPaths == null || inputPaths.length == 0){
                throw new RuntimeException("输入路径为空");
            }

            if(outputPath == null ){
                throw new RuntimeException("输出路径为空");
            }

            logger.info("====输入路径=====：" + inputPaths[0].toString());
            //hdfs://hadoop102:9000/mycompress/year=2018/month=06


            String[] fileNameSplit = inputPaths[0].getName().split("\\.");

            if(fileNameSplit.length == 0 ){
                throw new RuntimeException("输入路径 不是 *.* 格式");
            }

            String outFileName = outputPath.toString() + "/" + fileNameSplit[0] + DECOMPRESS_NAME;

            logger.info("====输出路径=====：" + outFileName);
            //FileSystem fs = FileSystem.get(context.getConfiguration());
            FileSystem fs = FileSystem.get(URI.create(inputPaths[0].toString()), context.getConfiguration());
            Path outPutPath = new Path(outFileName);
            //Path toCrawlPath = new Path(path2);
            FSDataOutputStream enhanceOut = fs.create(outPutPath);
            return new MyRecordWriter(enhanceOut);
        }

       /**
         * 泛型表示reduce输出的键值对类型；要保持一致
         */
         static class MyRecordWriter extends RecordWriter<Text, NullWritable> {

            FSDataOutputStream out ;

            public MyRecordWriter( FSDataOutputStream out) {
                this.out = out;
            }


            /**
             * 自定义输出kv对逻辑
             *
             * @param key
             * @param value
             * @throws IOException
             * @throws InterruptedException
             */
            @Override
            public void write(Text key, NullWritable value) throws IOException {

                if(key != null){
                    out.write(key.toString().getBytes());
                }


            }

            /**
             * 关闭流
             *
             * @param context
             * @throws IOException
             * @throws InterruptedException
             */
            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                if (out != null) {
                    out.close();
                }

            }
        }


    }


}
