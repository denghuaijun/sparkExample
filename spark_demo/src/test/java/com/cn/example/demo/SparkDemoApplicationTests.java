/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SparkDemoApplicationTests {

	@Autowired
    private JavaSparkContext sc;
	
	@Test
	public void contextLoads() {
		// 设置日志输出等级 有一些启动信息的日志没必要看
		sc.setLogLevel("WARN");
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = rdd.map(x -> (x * x));
		System.out.println(StringUtils.join(result.collect(), ","));
		sc.stop();
	}
}
