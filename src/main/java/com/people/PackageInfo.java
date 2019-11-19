package com.people;

public class PackageInfo {
    public static void main(String[] args){
        String a = "hdfs://hadoop102:9000/test/1-txt";


        String[] split = a.split("/");
        System.out.println(split);
    }
}
