package cn.itheima.bigdata.hadoop.mr.joinquery;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itheima.bigdata.hadoop.mr.ii.InverseIndexStepTwo;
import cn.itheima.bigdata.hadoop.mr.ii.InverseIndexStepTwo.InverseIndexStepTwoMapper;
import cn.itheima.bigdata.hadoop.mr.ii.InverseIndexStepTwo.InverseIndexStepTwoReducer;

public class JoinQuery {

	public static class JoinQueryMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {

			String record = value.toString();
			
			String[] fields = StringUtils.split(record,"\t");
			
			String id = fields[0];
			String name = fields[1];
			
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			
			k.set(id);
			v.set(name+"-->"+fileName);
			// k:001    v:  iphone6-->a.txt
			context.write(k, v);
		}
		
		
	}
	
	public static class JoinQueryReducer extends Reducer<Text, Text, Text, Text>{
		
		
		// k:001    values:  [iphone6-->a.txt, 00101-->b.txt,00110-->b.txt]
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			//第一次循环拿出a表中的那个字段
			String leftKey = "";
			ArrayList<String> rightFields = new ArrayList<>();
			for(Text value:values){
				if(value.toString().contains("a.txt")){
					leftKey = StringUtils.split(value.toString(), "-->")[0];
				}else{
					rightFields.add(value.toString());
				}
				
			}
			

			//再用leftkey去遍历拼接b表中的字段，并输出结果
			for(String field:rightFields){
					String result ="";
					result += leftKey +"\t" +StringUtils.split(field.toString(), "-->")[0];
					context.write(new Text(leftKey), new Text(result));
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		Job joinjob = Job.getInstance(conf);
		
		joinjob.setJarByClass(JoinQuery.class);
		
		joinjob.setMapperClass(JoinQueryMapper.class);
		joinjob.setReducerClass(JoinQueryReducer.class);
		
		joinjob.setOutputKeyClass(Text.class);
		joinjob.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(joinjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(joinjob, new Path(args[1]));
		
		joinjob.waitForCompletion(true);
		
	
	}
	
}
