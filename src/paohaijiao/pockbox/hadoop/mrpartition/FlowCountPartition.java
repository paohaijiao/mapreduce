package cn.itheima.bigdata.hadoop.mr.partition;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.itheima.bigdata.hadoop.mr.flowcount.FlowBean;

public class FlowCountPartition {

public static class FlowCountPartitionMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	private FlowBean flowBean = new FlowBean();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		 
			// 拿到一行数据
			String line = value.toString();
			// 切分字段
			String[] fields = StringUtils.split(line, "\t");
			// 拿到我们需要的若干个字段
			String phoneNbr = fields[1];
			long up_flow = Long.parseLong(fields[fields.length - 3]);
			long d_flow = Long.parseLong(fields[fields.length - 2]);
			// 将数据封装到一个flowbean中
			flowBean.set(phoneNbr, up_flow, d_flow);

			// 以手机号为key，将流量数据输出去
			context.write(new Text(phoneNbr), flowBean);
		 
	}
	
}
	
	
	
	public static class FlowCountPartitionReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		private FlowBean flowBean = new FlowBean();
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,Context context)
				throws IOException, InterruptedException {

			long up_flow_sum = 0;
			long d_flow_sum = 0;
			
			for(FlowBean bean:values){
				
				up_flow_sum += bean.getUp_flow();
				d_flow_sum += bean.getD_flow();
			}
			
			flowBean.set(key.toString(), up_flow_sum, d_flow_sum);
			
			context.write(key, flowBean);
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"flowpartjob");
		
		job.setJarByClass(FlowCountPartition.class);
		
		job.setMapperClass(FlowCountPartitionMapper.class);
		job.setReducerClass(FlowCountPartitionReducer.class);
		
		/**
		 * 加入自定义分区定义 ： AreaPartitioner
		 */
		job.setPartitionerClass(AreaPartitioner.class);
		
		/**
		 * 设置reduce task的数量，要跟AreaPartitioner返回的partition个数匹配
		 * 如果reduce task的数量比partitioner中分组数多，就会产生多余的几个空文件
		 * 如果reduce task的数量比partitioner中分组数少，就会发生异常，因为有一些key没有对应reducetask接收
		 * (如果reduce task的数量为1，也能正常运行，所有的key都会分给这一个reduce task)
		 * reduce task 或 map task 指的是，reuder和mapper在集群中运行的实例
		 */
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	
}
