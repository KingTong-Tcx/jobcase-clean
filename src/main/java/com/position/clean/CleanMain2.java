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

public class CleanMain2 {
	public static void main(String[] args) throws IOException, 
	ClassNotFoundException, InterruptedException {
		//设置控制台输出日志
		BasicConfigurator.configure();
		// 初始化Hadoop环境
		Configuration conf = new Configuration();
		/*
		//从hadoop读取“两个”参数
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//判断从虚拟机获取的是不是两个参数
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			 System.exit(2);
		}
		*/
		//定义一个新的Job，第一个参数是Hadoop的配置信息，第二个是Job的名字
		Job job = new Job(conf,"job");
		//设置主类
		job.setJarByClass(CleanMain2.class);
		//设置Mapper类
		job.setMapperClass(CleanMapper.class);
		//设置处理小文件的对象，默认是TextInputFormat.class
		job.setInputFormatClass(CombineTextInputFormat.class);
		//设置n个小文件之和不能大于2M
		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
		//设置n个小文件之和大于2M，需要满足n+1个小文件之和不能大于4M
		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
		//设置job输出数据的key类
		job.setOutputKeyClass(Text.class);
		//设置Job输出数据的value类
		job.setOutputValueClass(NullWritable.class);
		//设置输入路径
//		FileInputFormat.addInputPath(job, new Path("hdfs://hadoop:8020/data/JobData/20221221"));
		FileInputFormat.addInputPath(job, new Path("hdfs://hadoop:8020/data/JobData/20221221"));
		//FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		//设置输出路径
//		FileOutputFormat.setOutputPath(job, new Path("E:\\VMware\\develop\\out"));//本机：out文件夹不用创建，会自动创建，会报错已存在错误
		FileOutputFormat.setOutputPath(job, new Path("E:\\out"));//本机：out文件夹不用创建，会自动创建，会报错已存在错误
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//退出程序
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
