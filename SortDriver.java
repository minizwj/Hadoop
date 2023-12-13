package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortDriver {

    public static void main(String[] args) throws Exception {
        // 创建一个配置对象
        Configuration conf = new Configuration();

        // 创建一个Job对象
        Job job = Job.getInstance(conf, "DataSort");

        // 设置主类
        job.setJarByClass(SortDriver.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // 设置输出键和值的类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交Job并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

