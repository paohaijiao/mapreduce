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
 * 用来描述一个作业job（使用哪个mapper类，哪个reducer类，输入文件在哪，输出结果放哪。。。。）
 * 然后提交这个job给hadoop集群
 * @author duanhaitao@itcast.cn
 *
 */
//cn.itheima.bigdata.hadoop.mr.wordcount.WordCountRunner
public class WordCountRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job wcjob = Job.getInstance(conf);
		//设置job所使用的jar包
		conf.set("mapreduce.job.jar", "wcount.jar");
		
		//设置wcjob中的资源所在的jar包
		wcjob.setJarByClass(WordCountRunner.class);
		
		
		//wcjob要使用哪个mapper类
		wcjob.setMapperClass(WordCountMapper.class);
		//wcjob要使用哪个reducer类
		wcjob.setReducerClass(WordCountReducer.class);
	
		
		
		/**
Combiner组件：
1、是在每一个map task的本地运行，能收到map输出的每一个key的valuelist，所以可以做局部汇总处理
2、因为在map task的本地进行了局部汇总，就会让map端的输出数据量大幅精简，减小shuffle过程的网络IO
3、combiner其实就是一个reducer组件，跟真实的reducer的区别就在于，combiner运行maptask的本地
4、combiner在使用时需要注意，输入输出KV数据类型要跟map和reduce的相应数据类型匹配
5、要注意业务逻辑不能因为combiner的加入而受影响
		 * 
		 * 
		 */
		//指定本job所使用的combiner类定义 
		wcjob.setCombinerClass(WordCountReducer.class);
		
		//wcjob的mapper类输出的kv数据类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		
		//wcjob的reducer类输出的kv数据类型
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);
		
		//指定要处理的原始数据所存放的路径
		FileInputFormat.setInputPaths(wcjob, "hdfs://yun12-01:9000/wc/srcdata");
	
		//指定处理之后的结果输出到哪个路径
		FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://yun12-01:9000/wc/output"));
		
		boolean res = wcjob.waitForCompletion(true);
		
		System.exit(res?0:1);
		
		
	}
	
	
	
}
