package com.people.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * 上传到 HDFS
 * args0  本地文件  D:\fpy\rollback\09.txt
 *                 D:\fpy\VMware-workstation-full-15.5.0-14665864.exe
 * args1 hdfs上文件 hdfs://hadoop102:9000/test/vmware
 *
 */
public class CopyFileFromLocal {

    public static void main(String[] args){
        //获取本地磁盘路径
        String source = "D:\\compress\\1.txt";

        // 上传路径
        String destination = "hdfs://hadoop102:9000/test/1-txt";
        InputStream in = null;

        try {
             in = new BufferedInputStream(new FileInputStream(source));
             //HDFS 读写配置文件
            Configuration conf = new Configuration();

            FileSystem fileSystem = FileSystem.get(URI.create(destination), conf);
            //调用fileSystem的create方法返回的是 FSDataOutputStream对象
            //该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
            OutputStream out = fileSystem.create(new Path(destination));

            IOUtils.copyBytes(in,out,4096,true);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
