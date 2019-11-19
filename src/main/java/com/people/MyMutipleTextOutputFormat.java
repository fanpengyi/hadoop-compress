package com.people;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class MyMutipleTextOutputFormat extends MultipleTextOutputFormat<Text, IntWritable> {

    /**
     * 实现一个REDUCE一个文件，且文件名自定义。
     */
    @Override
    protected String generateLeafFileName(String name) {


        System.out.println("===========into MyMutipleTextOutputFormat ===== name : "+name);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        String format = simpleDateFormat.format(new Date());

        return  format + ".txt";
    }
}

