package com.acda.app.primero;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerApp extends Reducer<Text,IntWritable,Text,IntWritable>{
	 private IntWritable result = new IntWritable();
	 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		 String k = key.toString();
		 if(k.equals("TRUMP") || k.equals("DICTATOR") || k.equals("MAGA") || k.equals("IMPEACH") || k.equals("DRAIN") || k.equals("SWAP") || k.equals("CHANGE")) {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		 }
		}

}
