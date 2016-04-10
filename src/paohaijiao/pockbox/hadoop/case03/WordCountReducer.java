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
		
		//����һ���ۼӼ�����
		long count = 0;
		for(LongWritable value:values){
			
			count += value.get();
			
		}
		
		//���<���ʣ�count>��ֵ��
		context.write(key, new LongWritable(count));
		
	}
	
	

}
