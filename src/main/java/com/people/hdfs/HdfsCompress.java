package com.people.hdfs;


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

import java.io.*;
import java.net.URI;

/**
 * org.apache.hadoop.io.compress.BZip2Codec  61M - 21M
 * org.apache.hadoop.io.compress.GzipCodec    61M - 21M
 * com.hadoop.compression.lzo.LzopCodec     61M - 21M
 * com.hadoop.compression.lzo.LzoCodec
 * org.apache.hadoop.io.compress.SnappyCodec
 * org.apache.hadoop.io.compress.DefaultCodec
 */
public class HdfsCompress {
    private static Logger logger = LoggerFactory.getLogger(HdfsCompress.class);

    public static void main(String[] args) throws ClassNotFoundException {

        //压缩相关
        //压缩类
        String  codecClassName = "org.apache.hadoop.io.compress.BZip2Codec";
        Class<?> codecClass = Class.forName(codecClassName);
        //获取HDFS配置信息
        Configuration configuration = new Configuration();
        //反射方法
        //Java 反射机制生产压缩类对象
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, configuration);
        //本地文件路径
        //D:\\fpy\\rollback\\09_1571300872088.txt
        String source="hdfs://nserver:9000/data_center/crawl/new_article/year=2018/month=05/2018-05-23-1.txt";
        //目标文件路径
        String destination="hdfs://nserver:9000/data_center/crawl/new_article/compress/year=2018/month=05/2018-05-23-1.bz2";

        try {
            FileSystem fileSystem = FileSystem.get(URI.create(source), configuration);
            FSDataInputStream in = fileSystem.open(new Path(source));
            //InputStream in = new BufferedInputStream(new FileInputStream(source));
            //获取 FileSystem
            //通过文件系统获取输出流
            OutputStream out = fileSystem.create(new Path(destination));
            //对输出流的数据压缩
            CompressionOutputStream compressOut = codec.createOutputStream(out);

            IOUtils.copyBytes(in,compressOut,4096,true);
            logger.info("compress success!");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            logger.error("压缩异常！",e);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("压缩异常！",e);
        }


    }
}
