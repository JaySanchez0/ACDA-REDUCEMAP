package com.acda.app.primero;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class MapperApp extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String data = value.toString();
      StringTokenizer itr = new StringTokenizer(data);
      while (itr.hasMoreTokens()) {
    	String str = itr.nextToken().replace("{", " ").replace("}", " ").replace("[", " ").replace("]", " ").replace("null", "").replace(",", " ").replace("\"", " ");
        word.set(str);
        context.write(word, one);
      }
    }
}
