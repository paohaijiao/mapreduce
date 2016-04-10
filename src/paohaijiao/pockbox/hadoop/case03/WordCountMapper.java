package cn.itheima.bigdata.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		//��ȡ��һ���ļ�������
		String line = value.toString();
		//�з���һ�е�����Ϊһ����������
		String[] words = StringUtils.split(line, " ");
		//�������  <word,1>
		for(String word:words){
			
			context.write(new Text(word), new LongWritable(1));
			
		}
		
		
		
		
	}
	
	
	
	

}
