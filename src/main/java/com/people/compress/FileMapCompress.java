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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 * 用于将hadoop 数据压缩，占用 Yarn资源
 *
 * @author fanpengyi
 * @date 2019-11-13
 * @version 2.0
 *
 */
public class FileMapCompress {

    private static ResourceBundle resource = ResourceBundle.getBundle("app");


    private static final String COMPRESS_NAME = resource.getString("compress_name");
    private static final String COMPRESS_CLASS_NAME = resource.getString("compress_class_name");

    private static Logger logger = LoggerFactory.getLogger(MapCompress.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //判断以下，输入参数是否是1个，表示输入文件路径
        if (args.length != 1) {
            System.out.println("please check input Path!");
            System.exit(0);
        }

        logger.info("read file path :" +args[0]);
        //创建文本输入流 一次读取一行
        FileReader fileReader = new FileReader(args[0]);

        BufferedReader br = new BufferedReader(fileReader);

        String inputPath;
        while((inputPath = br.readLine())!= null){
            String year = null;
            String month = null;

            String[] inputSplit = inputPath.split("/");

            if(inputSplit.length == 0){
                logger.error("输入路径有误！");
                return;
            }
            // 拼接输出路径 /year=../month=..
            for (String path : inputSplit) {
                if(path.contains("year=")){
                    year = path;
                }else if(path.contains("month=")){
                    month = path;
                }
            }

            if(year == null || month == null){
                System.out.println("please check input Path!");
                System.exit(0);
            }

            String[] inputFileNameSplit = inputSplit[inputSplit.length - 1].split("\\.");

            if(inputFileNameSplit.length == 0){
                logger.error("输入路径有误,没有 带 .txt");
                return;
            }

            String FILE_OUTPUT_PATH = resource.getString("compress_file_output_path");

            FILE_OUTPUT_PATH =  FILE_OUTPUT_PATH.concat(year).concat("/").concat(month).concat("/").concat( inputFileNameSplit[0]);

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
            Job job = Job.getInstance(configuration, FileMapCompress.class.getSimpleName());
            //设置jar包，参数是包含main方法的类
            job.setJarByClass(FileMapCompress.class);

            //设置输入/输出路径
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(FILE_OUTPUT_PATH));

            //设置处理Map阶段的自定义的类
            job.setMapperClass(SearchCountMapper.class);

            job.setOutputFormatClass(MyOutPutFormat.class);

            // 设置自定义输出流修改文件名
            Class<?> clsClass = Class.forName(COMPRESS_CLASS_NAME);

            CompressionCodec codec = (CompressionCodec)
                    ReflectionUtils.newInstance(clsClass, configuration);
            MyOutPutFormat.setCodec(codec);

            //设置reduce task最终输出key/value的类型
            //注意：此处设置的reduce输出的key/value类型，一定要与自定义reduce类输出的kv对类型一致；否则程序运行报错
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            //设置没有 reduceTask
            //job.setNumReduceTasks(0);
            logger.info(   "reduceTask ----> 0 ");
            //job.setOutputFormatClass(MyMutipleTextOutputFormat.class);
            // 设置reduce端输出压缩开启
            FileOutputFormat.setCompressOutput(job, true);
            // 设置压缩的方式
            FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
            // 提交作业
            job.waitForCompletion(true);

            logger.info( inputPath + "    compress success!   ");

            TimeUnit.SECONDS.sleep(30);

        }
        logger.info(  " all path  compress success!   ");
       // System.exit(1);

    }

    public static class SearchCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        //定义共用的对象，减少GC压力
        Text userIdKOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获得当前行的数据
            String line = value.toString();
            userIdKOut.set(line);
            //输出结果
            context.write(userIdKOut, NullWritable.get());
        }
    }




    //建立一个类，继承TextOutputFormat，此class位于org.apache.hadoop.mapreduce.lib.output
    private static class MyOutPutFormat extends TextOutputFormat {

        static CompressionCodec codec;

        public static void setCodec(CompressionCodec newCodec) {
            codec = newCodec;
        }

        /**
         * 自定义文件名输出类
         * @param context 上下文
         */
        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
            Path[] inputPaths = FileInputFormat.getInputPaths(context);
            Path outputPath = FileOutputFormat.getOutputPath(context);

            if(inputPaths.length == 0){
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

            String outFileName = outputPath.toString() + "/" + fileNameSplit[0] + COMPRESS_NAME;
            logger.info("====输出路径=====：" + outFileName);
            //FileSystem fs = FileSystem.get(context.getConfiguration());
            FileSystem fs = FileSystem.get(URI.create(inputPaths[0].toString()), context.getConfiguration());
            Path outPutPath = new Path(outFileName);
            //Path toCrawlPath = new Path(path2);
            FSDataOutputStream enhanceOut = fs.create(outPutPath);

            if(codec == null){
                logger.info("====codec=====：为 null，需要新建codec" );
                Class<?> clsClass;
                try {
                    clsClass = Class.forName(COMPRESS_CLASS_NAME);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("自定义输出文件类创建压缩流失败");
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

            CompressionOutputStream out ;

            public MyRecordWriter( CompressionOutputStream out) {
                this.out = out;
            }


            /**
             * 自定义输出kv对逻辑
             *
             * @param key 输出的key
             * @param value 输出的值
             */
            @Override
            public void write(Text key, NullWritable value) throws IOException {

                if(key != null){
                    out.write((key.toString()+"\r\n").getBytes());
                }


            }

            /**
             * 关闭流
             *
             * @param context 上下文
             */
            @Override
            public void close(TaskAttemptContext context) throws IOException {
                if (out != null) {
                    out.close();
                }

            }
        }


    }


}
