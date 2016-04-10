package com.itcast.winjob;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {

	/**
	 * Mapper
	 * 
	 * **/
	private static class WMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String values[]=StringUtils.split(value.toString(),"\t");
			for(String word:values){
				context.write(new Text(word),new IntWritable(1));
			}
		}
		
	}

	/**
	 * Reducer
	 * 
	 * **/
	private static class WReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		private Text t = new Text();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable i : value) {
				count += i.get();
			}
			t.set(count + "");
			context.write(key, t);

		}

	}

	public static void printEnv(Job job) {
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "yun12-01");
		System.out.println("###########################################");
		System.out.println("fs.defaultFS:" + conf.get("fs.defaultFS"));
		System.out.println("mapred.job.tracker:"
				+ conf.get("mapred.job.tracker"));
		System.out.println("mapreduce.framework.name" + ":"
				+ conf.get("mapreduce.framework.name"));
		System.out.println("yarn.nodemanager.aux-services" + ":"
				+ conf.get("yarn.nodemanager.aux-services"));
		System.out.println("yarn.resourcemanager.address" + ":"
				+ conf.get("yarn.resourcemanager.address"));
		System.out.println("yarn.resourcemanager.scheduler.address" + ":"
				+ conf.get("yarn.resourcemanager.scheduler.address"));
		System.out.println("yarn.resourcemanager.resource-tracker.address"
				+ ":"
				+ conf.get("yarn.resourcemanager.resource-tracker.address"));
		System.out.println("yarn.application.classpath" + ":"
				+ conf.get("yarn.application.classpath"));
		System.out.println("zkhost:" + conf.get("zkhost"));
		System.out.println("namespace:" + conf.get("namespace"));
		System.out.println("project:" + conf.get("project"));
		System.out.println("collection:" + conf.get("collection"));
		System.out.println("shard:" + conf.get("shard"));
		System.out.println("###########################################");
	}
	 /**
	  * 载入hadoop的配置文件
	  * 
	  * 
	  * */
	public static void getConf(final Configuration conf) throws FileNotFoundException{
		String HADOOP_CONF_DIR = System.getenv().get("HADOOP_CONF_DIR");
		String HADOOP_HOME = System.getenv().get("HADOOP_HOME");
		System.out.println("HADOOP_HOME:" + HADOOP_HOME);
		System.out.println("HADOOP_CONF_DIR:" + HADOOP_CONF_DIR);//此处兼容hadoop1.x
		
		//此处兼容hadoop2.x
		if (HADOOP_CONF_DIR == null || HADOOP_CONF_DIR.isEmpty()) {
			HADOOP_CONF_DIR = HADOOP_HOME + "/etc/hadoop";
		}

		//得到hadoop的conf目录的路径加载文件
		File file = new File(HADOOP_CONF_DIR);
		FilenameFilter filter = new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith("xml");
			}
		};
		
		
		//获取hadoop的仅仅xml结尾的文件列表
		String[] list = file.list(filter);
		for (String fn : list) {
			System.out.println("Loading Configuration: " + HADOOP_CONF_DIR
					+ "/" + fn);
			//循环加载xml文件
			conf.addResource(new FileInputStream(HADOOP_CONF_DIR + "/" + fn));
		}

		 
		
		//yarn的classpath路径，如果为空则加载拼接yarn的路径
		if (conf.get("yarn.application.classpath", "").isEmpty()) {
			StringBuilder sb = new StringBuilder();
			sb.append(System.getenv("CLASSPATH")).append(":");
			sb.append(HADOOP_HOME).append("/share/hadoop/common/lib/*")
					.append(":");
			sb.append(HADOOP_HOME).append("/share/hadoop/common/*").append(":");
			sb.append(HADOOP_HOME).append("/share/hadoop/hdfs/*").append(":");
			sb.append(HADOOP_HOME).append("/share/hadoop/mapreduce/*")
					.append(":");
			sb.append(HADOOP_HOME).append("/share/hadoop/yarn/*").append(":");
			sb.append(HADOOP_HOME).append("/lib/*").append(":");
			conf.set("yarn.application.classpath", sb.toString());
		}
		
		
		
		
		
		
	}
	
 

	public static void main(String[] args) throws Exception { {
			 
			Configuration conf = new Configuration();
			conf.set("mapreduce.job.jar", "wcwin.jar");//此处代码，一定放在Job任务前面，否则会报类找不到的异常
			Job job = Job.getInstance(conf, "winjob");	 
			getConf(conf);
			job.setJarByClass(WordCount.class);
			job.setMapperClass(WMapper.class);
			job.setReducerClass(WReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			String path = "/wc/output";
//			FileSystem fs = FileSystem.get(conf);
			Path p = new Path(path);
//			if (fs.exists(p)) {
//				fs.delete(p, true);
//				System.out.println("输出路径存在，已删除！");
//			}
			          
			FileInputFormat.setInputPaths(job, "/wc/srcdata");
			FileOutputFormat.setOutputPath(job, p);
			printEnv(job);
			System.exit(job.waitForCompletion(true) ? 0 : 1); 
		 
	}

	}
}
