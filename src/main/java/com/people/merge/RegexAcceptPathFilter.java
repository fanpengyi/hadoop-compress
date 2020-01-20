package com.people.merge;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.PrintStream;

public class RegexAcceptPathFilter implements PathFilter {

    private final String regex;

    public RegexAcceptPathFilter(String regex)
    {
        this.regex = regex;
    }

    public boolean accept(Path path)
    {
        Pattern pattern = Pattern.compile(this.regex);
        Matcher matcher = pattern.matcher(path.getName());
        if (matcher.find()) {
            return true;
        }
        return false;
    }

    public static void main(String[] args)
    {
        String p = "hdfs://hadoop102:9000/data_center/weibo/year=2020/month=01/2020-01-11.txt";
        Path path = new Path(p);
        RegexAcceptPathFilter r = new RegexAcceptPathFilter("2020-01-11..*.txt");
        System.out.println(r.accept(path));
    }
}