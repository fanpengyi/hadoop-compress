package com.people.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.URI;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

public class HdfsDeCompress {

    public static ResourceBundle resource = ResourceBundle.getBundle("app");

    private static Logger logger  = LoggerFactory.getLogger(HdfsDeCompress.class);

    private static final String DECOMPRESS_NAME = resource.getString("compress_name");
    private static final String DECOMPRESS_CLASS_NAME = resource.getString("compress_class_name");


    public static final boolean USE_INNER_PARAM = Boolean.valueOf(resource.getString("use_out_param"));

    public static void main(String[] args) throws Exception {
        //压缩目录
           String INPUT_PATH =resource.getString("decompress_input_path");
           String OUTPUT_PATH = resource.getString("decompress_output_path");
        if(USE_INNER_PARAM){
            args = new String[2];
            // args[0] = "hdfs://nserver:9000/data_center/crawl/new_article/year=2018/month=06/2018-06-01-00.txt";
            // args[0] = "hdfs://hadoop102:9000/big";
            args[0] = INPUT_PATH;
            //args[1] = "D:\\compress\\_5_bzip2";
            args[1] = OUTPUT_PATH;
        }else{
            INPUT_PATH =  args[0];
            OUTPUT_PATH = args[1];
        }

        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        String[] inputPathSplits = INPUT_PATH.split("/");

        if(inputPathSplits.length == 0){
            logger.error("输入路径有误");
            return;
        }

        String[] fileNames = inputPathSplits[inputPathSplits.length - 1].split("\\.");

        if(fileNames.length == 0){
            logger.error("输入路径有误，没有包含 *.*");
            return;
        }
        OUTPUT_PATH = OUTPUT_PATH + "/" + fileNames[0] + DECOMPRESS_NAME;

        //解压目录
        //String deCompressUrl = "hdfs://hadoop102:9000/myDecompress/year=2018/month=11/09";
        //指定压缩方式
        Class<?> codecClass = Class.forName(DECOMPRESS_CLASS_NAME);

        Configuration conf = new Configuration();
       // FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(URI.create(INPUT_PATH),conf);

        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        CompressionInputStream in =null;
        //压缩文件路径
        in = codec.createInputStream(fs.open(new Path(INPUT_PATH)));

        //创建 HDFS 上的输出流
        OutputStream out = fs.create(new Path(OUTPUT_PATH));

        //读入压缩文件 写出到HDFS上
        IOUtils.copyBytes(in, out, 4096,true);
        //IOUtils.closeStream(in);

        System.out.println("success");

        TimeUnit.SECONDS.sleep(5);

    }



}
