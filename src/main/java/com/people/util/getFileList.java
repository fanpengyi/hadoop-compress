package com.people.util;

import com.people.hdfs.HdfsDeCompress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

public class getFileList {


    public static ResourceBundle resource = ResourceBundle.getBundle("app");

    private static Logger logger  = LoggerFactory.getLogger(HdfsDeCompress.class);

    private static final String FILE_LIST_OUTPUT_PATH = resource.getString("file_list_output_path");



    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.out.println("please check input Path!");
            System.exit(0);
        }

        List strings = new ArrayList<>();
        listAllFiles(args[0],strings);

        System.out.println("success!");

    }




    public static FileSystem getHdfs(String path) throws IOException {
        Configuration conf = new Configuration();
        return FileSystem.get(URI.create(path),conf);
    }


    public static Path[] getFilesAndDirs(String path) throws IOException {
        FileStatus[] fs = getHdfs(path).listStatus(new Path(path));
        return FileUtil.stat2Paths(fs);
    }

    public static void listAllFiles(String path, List<String> strings) throws IOException {
        FileSystem hdfs = getHdfs(path);
        Path[] filesAndDirs = getFilesAndDirs(path);

        for(Path p : filesAndDirs){
            if(hdfs.getFileStatus(p).isFile()){
                if(!p.getName().contains("SUCCESS")){
                    System.out.println(p);
                }
            }else{
                listAllFiles(p.toString(),strings);
            }
        }

       // FileUtils.writeLines(new File(FILE_LIST_OUTPUT_PATH), strings,true);


    }




}
