package com.acda.app.segundo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperApp extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String data = value.toString().replace("{", " ").replace("}", " ").replace("[", " ").replace("]", " ").replace("null", "").replace(",", " ").replace("\"", " ");
      StringTokenizer itr = new StringTokenizer(data);
      while (itr.hasMoreTokens()) {
    	String pal = itr.nextToken();
    	if(!pal.equals("an") && !pal.equals("a") && !pal.equals("and") && !pal.equals("are") && !pal.equals("or") && 
    		!pal.equals("but") && !pal.equals("then") && !pal.equals("that") && !pal.equals("stop")) {
	        word.set(pal);
	        context.write(word, one);
    	}
      }
    }
}
