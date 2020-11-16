/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkDemoApplication implements CommandLineRunner {

	@Autowired
	private JavaSparkContext sc;

	public static void main(String[] args) {
		SpringApplication.run(SparkDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		// RDD1.subtract(RDD2):返回一个新的RDD，内容是：RDD1中存在的，RDD2中不存在的
		List<String> list1 = new ArrayList<String>();
		list1.add("hello1");
		list1.add("hello2");
		list1.add("hello3");
		list1.add("hello4");

		List<String> list2 = new ArrayList<String>();
		list2.add("hello3");
		list2.add("hello4");
		list2.add("world5");
		list2.add("world6");

		JavaRDD<String> a = sc.parallelize(list1);
		JavaRDD<String> b = sc.parallelize(list2);
		JavaRDD<String> c = a.subtract(b);
		System.out.println(StringUtils.join(c.collect(), ","));
		sc.stop();
	}
}
