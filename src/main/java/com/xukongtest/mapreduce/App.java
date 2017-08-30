package com.xukongtest.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.xukongtest.mapreduce.AverageScore.AverageMapper;
import com.xukongtest.mapreduce.AverageScore.AverageReducer;

/**
 * 
 * @author xukong
 *
 */
public class App {
    /**
     * 
     * TODO 添加方法注释.
     * 
     * @param args
     *            .
     * @throws IOException
     *             .
     * @throws ClassNotFoundException
     *             .
     * @throws InterruptedException
     *             .
     */
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new String[2];
            otherArgs[0] = "hdfs://192.168.6.136:8020/user/hdfs/input.txt";
            otherArgs[1] = "hdfs://192.168.6.136:8020/user/hdfs/output/";
            Job job = new Job(conf, "Average Score");
            job.setJarByClass(AverageScore.class);
            job.setMapperClass(AverageMapper.class);
            job.setCombinerClass(AverageReducer.class);
            job.setReducerClass(AverageReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            //// 判断一下输出目录是否已经存在
            Path path = new Path(otherArgs[1]);
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
                //// 如果已经存在则删除
                boolean isDeleted = fs.delete(path);
                if (isDeleted) {
                    fs.close();
                }
            }

            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            System.out.println("Static over");
        } catch (Exception EX) {
            System.out.println(EX.getMessage());
        }
    }
}
