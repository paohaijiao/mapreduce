package cn.itheima.bigdata.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ��������һ����ҵjob��ʹ���ĸ�mapper�࣬�ĸ�reducer�࣬�����ļ����ģ����������ġ���������
 * Ȼ���ύ���job��hadoop��Ⱥ
 * @author duanhaitao@itcast.cn
 *
 */
//cn.itheima.bigdata.hadoop.mr.wordcount.WordCountRunner
public class WordCountRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job wcjob = Job.getInstance(conf);
		//����job��ʹ�õ�jar��
		conf.set("mapreduce.job.jar", "wcount.jar");
		
		//����wcjob�е���Դ���ڵ�jar��
		wcjob.setJarByClass(WordCountRunner.class);
		
		
		//wcjobҪʹ���ĸ�mapper��
		wcjob.setMapperClass(WordCountMapper.class);
		//wcjobҪʹ���ĸ�reducer��
		wcjob.setReducerClass(WordCountReducer.class);
	
		
		
		/**
Combiner�����
1������ÿһ��map task�ı������У����յ�map�����ÿһ��key��valuelist�����Կ������ֲ����ܴ���
2����Ϊ��map task�ı��ؽ����˾ֲ����ܣ��ͻ���map�˵����������������򣬼�Сshuffle���̵�����IO
3��combiner��ʵ����һ��reducer���������ʵ��reducer����������ڣ�combiner����maptask�ı���
4��combiner��ʹ��ʱ��Ҫע�⣬�������KV��������Ҫ��map��reduce����Ӧ��������ƥ��
5��Ҫע��ҵ���߼�������Ϊcombiner�ļ������Ӱ��
		 * 
		 * 
		 */
		//ָ����job��ʹ�õ�combiner�ඨ�� 
		wcjob.setCombinerClass(WordCountReducer.class);
		
		//wcjob��mapper�������kv��������
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		
		//wcjob��reducer�������kv��������
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);
		
		//ָ��Ҫ�����ԭʼ��������ŵ�·��
		FileInputFormat.setInputPaths(wcjob, "hdfs://yun12-01:9000/wc/srcdata");
	
		//ָ������֮��Ľ��������ĸ�·��
		FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://yun12-01:9000/wc/output"));
		
		boolean res = wcjob.waitForCompletion(true);
		
		System.exit(res?0:1);
		
		
	}
	
	
	
}
