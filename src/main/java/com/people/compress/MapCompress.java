package com.people.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 * 用于将hadoop 数据压缩 ，配置文件传参 或 外部传参
 *
 * @author fanpengyi
 * @date 2019-11-13
 * @version 1.0
 *
 */
public class MapCompress {

    private static ResourceBundle resource = ResourceBundle.getBundle("app");

    private static final String COMPRESS_NAME = resource.getString("compress_name");

    private static final String COMPRESS_CLASS_NAME = resource.getString("compress_class_name");

    private static Logger logger = LoggerFactory.getLogger(MapCompress.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //判断以下，输入参数是否是两个，分别表示输入路径、输出路径


        String INPUT_PATH = resource.getString("compress_input_path");
        String OUTPUT_PATH = resource.getString("compress_output_path");

        boolean USE_INNER_PARAM = Boolean.valueOf(resource.getString("use_out_param"));


        if (USE_INNER_PARAM) {
            args = new String[2];
            // args[0] = "hdfs://nserver:9000/data_center/crawl/new_article/year=2018/month=06/2018-06-01-00.txt";
            // args[0] = "hdfs://hadoop102:9000/big";
            args[0] = INPUT_PATH;
            //args[1] = "D:\\compress\\_5_bzip2";
            args[1] = OUTPUT_PATH;
        } else {
            INPUT_PATH = args[0];
            OUTPUT_PATH = args[1];
        }


        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        String[] inputSplit = args[0].split("/");

        if (inputSplit == null || inputSplit.length == 0) {
            logger.error("输入路径有误！");
            return;
        }

        String[] inputFileNameSplit = inputSplit[inputSplit.length - 1].split("\\.");

        if (inputFileNameSplit == null || inputFileNameSplit.length == 0) {
            logger.error("输入路径有误,没有 带 .txt");
            return;
        }

        args[1] = args[1] + "/" + inputFileNameSplit[0];


        Configuration configuration = new Configuration();

        //开启map输出进行压缩的功能
        configuration.set("mapreduce.map.output.compress", "true");
        //设置map输出的压缩算法是：BZip2Codec，它是hadoop默认支持的压缩算法，且支持切分
        configuration.set("mapreduce.map.output.compress.codec", COMPRESS_CLASS_NAME);
        //开启job输出压缩功能
        configuration.set("mapreduce.output.fileoutputformat.compress", "true");
        //指定job输出使用的压缩算法
        configuration.set("mapreduce.output.fileoutputformat.compress.codec", COMPRESS_CLASS_NAME);

        //调用getInstance方法，生成job实例
        Job job = Job.getInstance(configuration, MapCompress.class.getSimpleName());
        //设置jar包，参数是包含main方法的类
        job.setJarByClass(MapCompress.class);

        //通过job设置输入/输出格式
        //MR的默认输入格式是TextInputFormat，所以下两行可以注释掉
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置处理Map阶段的自定义的类
        job.setMapperClass(SearchCountMapper.class);
        //设置map combine类，减少网路传出量
        //设置处理Reduce阶段的自定义的类
        //job.setReducerClass(SearchCountReducer.class);

        //如果map、reduce的输出的kv对类型一致，直接设置reduce的输出的kv对就行；如果不一样，需要分别设置map, reduce的输出的kv类型
        //注意：此处设置的map输出的key/value类型，一定要与自定义map类输出的kv对类型一致；否则程序运行报错
        // job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(NullWritable.class);
        //job.setOutputFormatClass(MyOutPutFormat.class);
        job.setOutputFormatClass(MyOutPutFormat.class);

        // 设置自定义输出流修改文件名
        String codecClassName = COMPRESS_CLASS_NAME;

        Class<?> clsClass = Class.forName(codecClassName);

        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(clsClass, configuration);
        MyOutPutFormat.setCodec(codec);
        //设置没有 reduceTask
       // job.setNumReduceTasks(0);
        //设置reduce task最终输出key/value的类型
        //注意：此处设置的reduce输出的key/value类型，一定要与自定义reduce类输出的kv对类型一致；否则程序运行报错
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //job.setOutputFormatClass(MyMutipleTextOutputFormat.class);
        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        // 提交作业
        job.waitForCompletion(true);


        logger.info("输入路径为：" + args[0] + "    compress ---->   " + "输出路径" + args[1]);


        logger.info("    compress success!   ");

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


    //建立一个类，继承TextOutputFormat，此class位于org.apache.hadoop.mapreduce.lib.output
    private static class MyOutPutFormat extends TextOutputFormat {

        static CompressionCodec codec;

        static CompressionOutputStream out;

        public static void setCodec(CompressionCodec newCodec) {
            codec = newCodec;
        }

        /**
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

            if (inputPaths.length == 0) {
                throw new RuntimeException("输入路径为空");
            }

            if (outputPath == null) {
                throw new RuntimeException("输出路径为空");
            }

            logger.info("====输入路径=====：" + inputPaths[0].toString());
            //hdfs://hadoop102:9000/mycompress/year=2018/month=06

            String[] fileNameSplit = inputPaths[0].getName().split("\\.");

            if (fileNameSplit.length == 0) {
                throw new RuntimeException("输入路径 不是 *.* 格式");
            }

            String outFileName = outputPath.toString() + "/" + fileNameSplit[0] + COMPRESS_NAME;
            logger.info("====输出路径=====：" + outFileName);
            //FileSystem fs = FileSystem.get(context.getConfiguration());
            FileSystem fs = FileSystem.get(URI.create(inputPaths[0].toString()), context.getConfiguration());
            Path outPutPath = new Path(outFileName);
            //Path toCrawlPath = new Path(path2);
            FSDataOutputStream enhanceOut = fs.create(outPutPath);


            if (codec == null) {
                logger.info("====codec=====：为 null，需要新建codec");
                String codecClassName = COMPRESS_CLASS_NAME;

                Class<?> clsClass = null;
                try {
                    clsClass = Class.forName(codecClassName);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                codec = (CompressionCodec)
                        ReflectionUtils.newInstance(clsClass, context.getConfiguration());
            }

            CompressionOutputStream out = codec.createOutputStream(enhanceOut);

            //显示赋值为null，用于垃圾回收
            codec = null;
            // FSDataOutputStream toCrawlOut = fs.create(toCrawlPath);
            return new MyRecordWriter(out);
        }

        /**
         * 泛型表示reduce输出的键值对类型；要保持一致
         */
        static class MyRecordWriter extends RecordWriter<Text, NullWritable> {

            CompressionOutputStream out;

            public MyRecordWriter(CompressionOutputStream out) {
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

                if (key != null) {
                    //System.out.print(key.toString());
                    out.write((key.toString() + "\r\n").getBytes());
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
