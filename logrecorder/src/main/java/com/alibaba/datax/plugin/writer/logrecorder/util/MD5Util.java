/** 
 * Project Name:jdbcTestProj 
 * File Name:MD5Util.java 
 * Package Name:com.h3c.dataExtraction.utils 
 * Date:2017年10月9日下午2:51:20 
 * Copyright (c) 2017, chenyuqg@hotmail.com All Rights Reserved. 
 * 
*/  
  
package com.alibaba.datax.plugin.writer.logrecorder.util;

import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/** 
 * ClassName:MD5Util <br/> 
 * Function: TODO ADD FUNCTION. <br/> 
 * Reason:   TODO ADD REASON. <br/> 
 * Date:     2017年10月9日 下午2:51:20 <br/> 
 * @author   YQ 
 * @version   
 * @since    JDK 1.6 
 * @see       
 */
public class MD5Util {

	public static String getMD5(String inputStr) {
		char hexDigits[] =  {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D','E', 'F'}; 
		try {
			byte[] byInput = inputStr.getBytes("utf-8");
			MessageDigest mdInst = MessageDigest.getInstance("MD5");
			mdInst.update(byInput);
			byte[] md = mdInst.digest();
			int j = md.length;
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte byte0 = md[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			}
			return new String(str);
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}


	/**
	 * 获取随机的5位数字
	 * @return
	 */
	public static int get5RanNum(){
		int rtnVal = (int)(Math.random()*9+1)*10000;
		return rtnVal;
	}

	public static String getTime5RanNum(){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		Random random = new Random();
		int rannum = (int) (random.nextDouble() * (99999 - 10000 + 1)) + 10000;// 获取5位随机数
		return sdf.format(new Date()) + rannum;
	}

	public static String getCurrentTime(){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return sdf.format(new Date());
	}

	public static void main(String[] args) {
		//System.out.println(getMD5("ILOVEYOU"));
		System.out.println(getTime5RanNum());
	}

}
