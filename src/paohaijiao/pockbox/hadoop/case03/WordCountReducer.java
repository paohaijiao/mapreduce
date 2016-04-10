package cn.itheima.bigdata.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	
	// key: hello ,  values : {1,1,1,1,1.....}
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {
		
		//定义一个累加计数器
		long count = 0;
		for(LongWritable value:values){
			
			count += value.get();
			
		}
		
		//输出<单词：count>键值对
		context.write(key, new LongWritable(count));
		
	}
	
	

}
