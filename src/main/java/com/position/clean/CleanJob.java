package com.position.clean;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class CleanJob {
	/**
	 * 删除字符串中指定字符
	 * @param str 要处理的字符串
	 * @param delChar 要删除的字符
	 * @return
	 */
	public static String deleteString(String str,char delChar) {
		StringBuffer stringBuffer = new StringBuffer("");
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) != delChar) {
				stringBuffer.append(str.charAt(i));
			}
		}
		return stringBuffer.toString();
	}
	
	/**
	 * 合并两个福利标签的字段内容
	 * @param position
	 * @param company  JsonArray数据
	 * @return
	 * @throws JSONException
	 */
	public static String mergeString(String position,JSONArray company) throws JSONException {
		String result = "";
		if (company.length() != 0) {
			for (int i = 0; i < company.length(); i++) {
				result = result + company.get(i) + "-";
			}
		}
		if(position != "") {
			String[] split = position.split(" |; |,|、|；|/");
			for (int i = 0; i < split.length; i++) {
				result = result + split[i].replaceAll("[\\pP\\p{Punct}]", "") + "-";
			}
		}
		return result.substring(0,result.length()-1);
	}
	
	/**
	 * 处理技能标签
	 * @param killData
	 * @return
	 * @throws JSONException
	 */
	public static String killResult(JSONArray killData) throws JSONException {
		String result = "";
		if(killData.length() != 0) {
			for (int i = 0; i < killData.length(); i++) {
				result = result + killData.get(i) + "-";
			}
			return result = result.substring(0,result.length()-1);
		}
		return result;
	}
	
	/**
	 * 数据清洗结果
	 * @param jobdata 待处理的职位信息数据
	 * @return
	 * @throws JSONException
	 */
	public static String resultToString(JSONArray jobdata) throws JSONException {
		String jobResultData = "";
		for (int i = 0; i < jobdata.length(); i++) {
			//获取每条职位信息
			String everyData = jobdata.get(i).toString();
			//String类型的数据转为JSON对象
			JSONObject everyDataJson = new JSONObject(everyData);
			//获取职位信息中的城市数据
			String city = everyDataJson.getString("city");
			//获取职位信息中的薪资数据
			String salary = everyDataJson.getString("salary");
			//获取职位信息中的福利标签数据
			String positionAdvantage = everyDataJson.getString("positionAdvantage");
			//获取职位信息中的福利标签数据
			JSONArray companyLabelList = everyDataJson.getJSONArray("companyLabelList");
			//获取职位信息中的技能标签数据
			JSONArray skillLables = everyDataJson.getJSONArray("skillLables");
			//处理薪资字段数据
			String salaryNew = deleteString(salary, 'k');
			String welfare = mergeString(positionAdvantage, companyLabelList);
			String kill = killResult(skillLables);
			//判断是否为最后一行
			if (i == jobdata.length()-1) {
				jobResultData = jobResultData + city + "," + salaryNew + "," + welfare + "," + kill;
			} else {
				jobResultData = jobResultData + city + "," + salaryNew + "," + welfare + "," + kill + "\n";
			}
		}
		return jobResultData;
	}


}
