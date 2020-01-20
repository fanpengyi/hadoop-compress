package com.people.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author fanpengyi
 * @version 1.0
 * @date 2020/1/20.
 *
 *
 * 将不同文件的内容合并到同一个文件中
 *
 *  将HDFS 上每一小时落地的文件，滚动生成一个大文件
 *
 */
public class MergeFile {

    private static Logger log = Logger.getLogger(MergeFile.class);
    private static Configuration conf = new Configuration();

    public static void Merge(String srcPath, String dstFile, String regex, boolean delete) {
        FileSystem fileSystem = null;
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        try {
            //获取文件系统
            fileSystem = FileSystem.get(conf);
            //原路径
            Path source = new Path(srcPath + "/*");
            log.info("source path: " + source.toString());
            //目标路径
            Path dest = new Path(dstFile);
            log.info("output file: " + dest.toString());
            log.info("regex expression: " + regex);
            if (delete) {
                log.info("This will delete all source files after merge!");
            }
            //正则获取文件列表
            FileStatus[] localStatus = fileSystem.globStatus(source, new RegexAcceptPathFilter(regex));
            //获取路径下所有文件
            Path[] files = FileUtil.stat2Paths(localStatus);
            if (files.length == 0) {
                log.info("no matcher files in " + srcPath);
            } else {
                //如果有目标文件 先删除
                if (fileSystem.exists(dest)) {
                    fileSystem.delete(dest, false);
                    log.info("delete file" + dest.toString());
                }
                //输出路径
                Path outPath = new Path(dstFile);
                //输出流
                out = fileSystem.create(outPath);
                long start = System.currentTimeMillis();
                //遍历文件 逐个写出
                for (Path file : files) {
                    log.info("Starting merge " + file.getName());

                    in = fileSystem.open(file);

                    IOUtils.copyBytes(in, out, conf, false);

                    in.close();
                    log.info("end merge " + file.getName());
                }
                log.info("merge is done, time:" + (System.currentTimeMillis() - start) + "ms");
                //如果有删除标识  将原来的文件删除
                if (delete) {
                    for (Path file : files) {
                        fileSystem.delete(file, false);
                    }
                }
            }
            return;
        } catch (IOException e) {
            e.printStackTrace();
            log.info("merge failed!");
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     * 参数含义
     * /data_center/article/year=${year}/month=${month}/
     * /data_center/article/year=${year}/month=${month}/${yestoday}.txt
     * ${yestoday}.*.txt
     * true
     *
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args)
            throws IOException {
        if (args.length < 3) {
            log.info("usage: hadoop jar merge.jar Merge srcPath destPath regex isDeleteSrcFile(default false)");
            return;
        }
        boolean delete = false;
        if (args.length == 4) {
            delete = Boolean.parseBoolean(args[3]);
        }
        //第一个参数是文件路径
        //第二个参数是目标文件路径，即滚动生成的大文件路径
        //第三个参数是 小文件的正则表达式
        //第四个参数是 是否删除
        Merge(args[0], args[1], args[2], delete);
    }
}
