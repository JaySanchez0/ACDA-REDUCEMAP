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
      String data = value.toString().replace("{", " ").replace("}", " ").replace("[", " ").replace("]", " ").replace("null", "").replace(",", " ").replace("\"", " ");
      StringTokenizer itr = new StringTokenizer(data);
      while (itr.hasMoreTokens()) {
    	  String k = itr.nextToken();
    	  if(k.equals("TRUMP") || k.equals("DICTATOR") || k.equals("MAGA") || k.equals("IMPEACH") || k.equals("DRAIN") || k.equals("SWAP") || k.equals("CHANGE")) {
    		 word.set(k);
    		 context.write(word, one);
    	  }
      }
    }
}
