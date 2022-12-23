package com.position.clean;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.util.ajax.JSON;


public class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String jobResultData = "";
		//将每个数据文件的内容转换为String类型
		String reptileData = value.toString();
		//通过截取字符串的方式获取content中的数据
		String jobData = reptileData.substring(reptileData.indexOf("=", reptileData.indexOf("=")+1)+1, reptileData.length() - 1);
		try {
			//先将String类型转换成JSONObject对象后，获取content中的数据内容
			JSONObject contentJson = new JSONObject(jobData);
			String contentData = contentJson.getString("content");
			//先将String类型转换成JSONObject对象后，获取content下的positionResult中的数据内容
			JSONObject positionResultJson = new JSONObject(contentData);
			String positionResultData = positionResultJson.getString("positionResult");
			//先将String类型转换成JSONObject对象后，获取最终的result中的数据内容
			JSONObject resultJson = new JSONObject(positionResultData);  
			JSONArray resultData = resultJson.getJSONArray("result");
			//对获得的数据进行处理，调用CleanJob类
			jobResultData = CleanJob.resultToString(resultData);
			//输出，获取空值只能NullWritable.get()来获取，不能使用new NullWritable()来定义
			context.write(new Text(jobResultData), NullWritable.get());
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	

}
