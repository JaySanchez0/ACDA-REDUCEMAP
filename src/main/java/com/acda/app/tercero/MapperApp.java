package com.acda.app.tercero;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONObject;

public class MapperApp extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      for (String val: value.toString().split("\n")) {
    	JSONObject obj = new JSONObject(val);
    	int id = obj.getJSONObject("user").getInt("id");
        word.set(String.valueOf(id));
        context.write(word, one);
      }
    }
}
