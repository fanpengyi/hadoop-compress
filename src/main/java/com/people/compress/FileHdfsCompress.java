package com.people.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;
/**
 * 用于将hadoop 数据压缩 ,非MR程序 不占用Yarn资源
 *
 * @author fanpengyi
 * @date 2019-11-13
 * @version 3.0
 *
 */
public class FileHdfsCompress {

    public static ResourceBundle resource = ResourceBundle.getBundle("app");

    private static Logger logger  = LoggerFactory.getLogger(FileHdfsCompress.class);

    private static final String COMPRESS_NAME = resource.getString("compress_name");
    private static final String COMPRESS_CLASS_NAME = resource.getString("compress_class_name");


    public static void main(String[] args) throws Exception {
        //压缩目录
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
            logger.info("输入路径："+inputPath);
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
                System.out.println("please check input Path! not contains year=* / month=*");
                System.exit(0);
            }

            String[] inputFileNameSplit = inputSplit[inputSplit.length - 1].split("\\.");

            if(inputFileNameSplit.length == 0){
                logger.error("输入路径有误,没有 带  .txt");
                return;
            }

            String FILE_OUTPUT_PATH = resource.getString("compress_file_output_path");

            FILE_OUTPUT_PATH =  FILE_OUTPUT_PATH.concat(year).concat("/").concat(month).concat("/").concat( inputFileNameSplit[0]).concat(COMPRESS_NAME);
            logger.info("输入路径："+inputPath+" begin compress .......");
            //解压目录
            //String deCompressUrl = "hdfs://hadoop102:9000/myDecompress/year=2018/month=11/09";
            //指定压缩方式
            Class<?> codecClass = Class.forName(COMPRESS_CLASS_NAME);

            Configuration conf = new Configuration();
            CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
            // FileSystem fs = FileSystem.get(conf);
            FileSystem fs = FileSystem.get(URI.create(inputPath),conf);

            //原文件路径 txt 用原本的输入流读入
            FSDataInputStream in = fs.open(new Path(inputPath));

            //创建 HDFS 上的输出流,压缩路径
            //通过文件系统获取输出流
            OutputStream out = fs.create(new Path(FILE_OUTPUT_PATH));
            //对输出流的数据压缩
            CompressionOutputStream compressOut = codec.createOutputStream(out);
            //读入原文件 压缩到HDFS上 输入--普通流  输出-压缩流
            IOUtils.copyBytes(in, compressOut, 4096,true);
            //IOUtils.closeStream(in);
            logger.info("输入路径："+inputPath + " compress ----> 输出路径："+FILE_OUTPUT_PATH+" ,result ----> success! ");
            TimeUnit.SECONDS.sleep(10);
        }

        logger.info( "all path compress success! ");

    }



}
