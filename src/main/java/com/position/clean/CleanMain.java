package com.position.clean;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import com.position.clean.CleanMapper;

public class CleanMain {
	public static void main(String[] args) throws IOException, 
	ClassNotFoundException, InterruptedException {
		 //控制台输出日志
		 BasicConfigurator.configure();
		 //初始化hadoop配置
		 Configuration conf = new Configuration();
		 //从hadoop命令行读取参数
		 String[] otherArgs = new GenericOptionsParser(conf, args)
				 .getRemainingArgs();
		// 判断从命令行读取的参数正常是两个，分别是输入文件和输出文件的目录
		 if(otherArgs.length != 2) {
			 System.err.println("Usage: wordcount <in> <out>");
			 System.exit(2);
		 }	 
		 //定义一个新的Job，第一个参数是hadoop配置信息，第二个参数是Job的名字
		 Job job = new Job(conf, "job");
		 //设置主类
		 job.setJarByClass(CleanMain.class);
		 //设置Mapper类
		 job.setMapperClass(CleanMapper.class);
		 //处理小文件，默认是TextInputFormat.class
		 job.setInputFormatClass(CombineTextInputFormat.class);
		 //n个小文件之和不能大于2M
		 CombineTextInputFormat.setMinInputSplitSize(job, 2097152);//2M
		 //在n个小文件之和大于2M情况下,需满足n+1个小文件之和不能大于4M
		 CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);//4M
		 //设置job输出数据的key类
		 job.setOutputKeyClass(Text.class);
		 //设置job输出数据的value类
		 job.setOutputValueClass(NullWritable.class);
		 //设置输入文件
		 FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		 //设置输出文件
		 FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
