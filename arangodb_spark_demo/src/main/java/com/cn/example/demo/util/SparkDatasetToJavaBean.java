/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


/**
 * 
 * Description: Dataset<Row> 转java对象
 * @author LiuHao
 * @date 2020年5月14日下午1:36:06
 * @version 1.0
 */
public class SparkDatasetToJavaBean {
	
	/**
	 * Description: 通过集合流进行对象转换
	 * @param <T>
	 * @param dataset
	 * @param class1
	 * @return
	 */
	public static <T> List<T> getStreamToJavaBean(Dataset<Row> dataset, Class<T> class1) {
		List<String> jsonList = dataset.toJSON().collectAsList();
		List<T> list = (List<T>) jsonList.stream()
				.map(jsonString -> JSON.parseObject(jsonString, class1))
				.collect(Collectors.toList());
		return list;
	}
	
	/**
	 * Description: 使用Encoders转对象
	 * @param <T>
	 * @param dataset
	 * @param class1
	 * @return
	 */
	public static <T> List<T> getEncodersToJavaBean(Dataset<Row> dataset, Class<T> class1) {
		List<T> list = dataset.as(Encoders.bean(class1)).collectAsList();
		return list;
	}

	/**
	 * Description: 通过JSONObject转对象
	 * @param <T>
	 * @param dataset
	 * @param class1
	 * @return
	 */
	public static <T> List<T> getJSONObjectToJavaBean(Dataset<Row> dataset, Class<T> class1) {
		List<T> list = new ArrayList<T>();
		String[] columns = dataset.columns();
		List<Row> rows = dataset.collectAsList(); 
		for (Row row : rows) {
			JSONObject json = new JSONObject();
			for (int i = 0; i < columns.length; i++) {
				json.put(columns[i], row.get(i));
			}
			list.add((T) JSONObject.toJavaObject(json, class1));
		}
		return list;
	}
}
