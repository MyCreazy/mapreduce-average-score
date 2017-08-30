/*
 * 文件名：AverageScore.java
 * 版权：Copyright 2007-2017 517na Tech. Co. Ltd. All Rights Reserved. 
 * 描述： AverageScore.java
 * 修改人：xukong
 * 修改时间：Aug 29, 2017
 * 修改内容：新增
 */
package com.xukongtest.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * TODO 添加类的一句话简单描述.
 * <p>
 * TODO 详细描述
 * <p>
 * TODO 示例代码
 * 
 * <pre>
 * </pre>
 * 
 * @author xukong
 */
public class AverageScore {
    /**
     * 
     * @author xukong .
     *
     */
    public static class AverageMapper extends Mapper<Object, Text, Text, FloatWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokens = new StringTokenizer(line, "\n");
            while (tokens.hasMoreTokens()) {
                String tmp = tokens.nextToken();
                StringTokenizer sz = new StringTokenizer(tmp);
                String name = sz.nextToken();
                float score = Float.valueOf(sz.nextToken());
                Text outName = new Text(name);
                FloatWritable outScore = new FloatWritable(score);
                context.write(outName, outScore);
            }
        }

    }

    /**
     * 
     * @author xukong .
     *
     */
    public static class AverageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable f : value) {
                sum += f.get();
                count++;
            }
            FloatWritable averageScore = new FloatWritable(sum / count);
            context.write(key, averageScore);
        }
    }
}
