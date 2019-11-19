package com.people.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class DownLoadFormHDFS {
    public static void main(String[] args){

        //HDFS 下载路径
        String sourceUrl  = "hdfs://hadoop102:9000/test/02.txt";

        //本地生成路径
        String  targetUrl = "D:/12345.txt";

        //获取HDFS 配置
        Configuration configuration = new Configuration();
        try {
            //创建 FileSystem
            FileSystem fileSystem = FileSystem.get(URI.create(sourceUrl), configuration);
            //FileSystem 获取 输入流
            InputStream in = fileSystem.open(new Path(sourceUrl));
            //获取本地输出流
            OutputStream out = new BufferedOutputStream(new FileOutputStream(targetUrl));

            //IOUtils 写出到本地
            IOUtils.copyBytes(in,out,4096,true);
            System.out.println("success");
        } catch (IOException e) {
            e.printStackTrace();
        }













    }
}
