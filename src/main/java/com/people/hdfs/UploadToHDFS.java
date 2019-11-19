package com.people.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class UploadToHDFS {
    public static void main(String[] args){

        String sourceUrl = "D:\\fpy\\rollback\\result2\\1.txt";
        String targetUrl = "hdfs://hadoop102:9000/test/02.txt";

        try {
            // 获取输入流
            InputStream in = new BufferedInputStream(new FileInputStream(sourceUrl));
            // 获取HDFS 配置
            Configuration configuration = new Configuration();
            //创建文件系统 FileSystem
            FileSystem fileSystem = FileSystem.get(URI.create(targetUrl), configuration);
            //根据文件系统创建输出流
            OutputStream out = fileSystem.create(new Path(targetUrl));
            //写出到Hdfs
            IOUtils.copyBytes(in,out,4096,true);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
