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
		 
			// �õ�һ������
			String line = value.toString();
			// �з��ֶ�
			String[] fields = StringUtils.split(line, "\t");
			// �õ�������Ҫ�����ɸ��ֶ�
			String phoneNbr = fields[1];
			long up_flow = Long.parseLong(fields[fields.length - 3]);
			long d_flow = Long.parseLong(fields[fields.length - 2]);
			// �����ݷ�װ��һ��flowbean��
			flowBean.set(phoneNbr, up_flow, d_flow);

			// ���ֻ���Ϊkey���������������ȥ
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
		 * �����Զ���������� �� AreaPartitioner
		 */
		job.setPartitionerClass(AreaPartitioner.class);
		
		/**
		 * ����reduce task��������Ҫ��AreaPartitioner���ص�partition����ƥ��
		 * ���reduce task��������partitioner�з������࣬�ͻ��������ļ������ļ�
		 * ���reduce task��������partitioner�з������٣��ͻᷢ���쳣����Ϊ��һЩkeyû�ж�Ӧreducetask����
		 * (���reduce task������Ϊ1��Ҳ���������У����е�key����ָ���һ��reduce task)
		 * reduce task �� map task ָ���ǣ�reuder��mapper�ڼ�Ⱥ�����е�ʵ��
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
