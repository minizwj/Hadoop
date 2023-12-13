package com.example;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable data = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取每行数据
        String line = value.toString();

        // 将数据转换成IntWritable类型
        data.set(Integer.parseInt(line));

        // 发送数据到Reducer
        context.write(data, value);
    }
}
