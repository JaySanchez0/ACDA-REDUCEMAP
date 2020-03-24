package com.acda.app.tercero;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReducerApp extends Reducer<Text,IntWritable,Text,IntWritable>{
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      for(String pal : value.toString().split("\n")) {
    	JSONObject json = new JSONObject(pal);
    	int id = json.getJSONObject("user").getInt("id");
        word.set(String.valueOf(id));
        context.write(word, one);
      }
    }
}
