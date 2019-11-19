package com.people.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class GetHDFSPath {


    public static void main(String[] args) throws IOException {

        String hdfsPath = "hdfs://hadoop102:9000/test/";

        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);

        String folderPath = "hdfs://hadoop102:9000/test/";
        Path path = new Path(folderPath);

        List<Path> filesUnderFolder = getFilesUnderFolder(fileSystem, path, null);


        for (Path path1 : filesUnderFolder) {
            System.out.println(path1.getName());
        }



    }



    /**
     * 得到一个目录(不包括子目录)下的所有名字匹配上pattern的文件名
     * @param fs
     * @param folderPath
     * @param pattern 用于匹配文件名的正则
     * @return
     * @throws IOException
     */
    public static List<Path> getFilesUnderFolder(FileSystem fs, Path folderPath, String pattern) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        if (fs.exists(folderPath)) {
            FileStatus[] fileStatus = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatus.length; i++) {
                FileStatus fileStatu = fileStatus[i];
                if (!fileStatu.isDir()) {//只要文件
                    Path oneFilePath = fileStatu.getPath();
                    if (pattern == null) {
                        paths.add(oneFilePath);
                    } else {
                        if (oneFilePath.getName().contains(pattern)) {
                            paths.add(oneFilePath);
                        }
                    }
                }else{

                }
            }
        }
        return paths;
    }

}
