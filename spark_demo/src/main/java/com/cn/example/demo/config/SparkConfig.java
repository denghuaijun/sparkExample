/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SparkConfig {

	@Value("${spark.appName}")
	private String appName;

	@Value("${spark.master}")
    private String master;
	
	@Value("${spark.executor.memory}")
    private String executorMemory;
	
	@Value("${spark.scheduler.mode}")
    private String schedulerMode;
	
	@Value("${spark.cores.max}")
    private String coresMax;
	
	@Value("${spark.deploy.defaultCores}")
    private String deployDefaultCores;
	
	@Value("${spark.logLevel}")
    private String logLevel;
	
	@Bean
	@ConditionalOnMissingBean(SparkConf.class)
	public SparkConf sparkConf() throws Exception {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		return conf;
	}

	@Bean
	@ConditionalOnMissingBean
	public JavaSparkContext javaSparkContext() throws Exception {
		JavaSparkContext sc = new JavaSparkContext(sparkConf());
		sc.setLogLevel(logLevel);
		return sc;
	}
	
	@Bean
	@ConditionalOnMissingBean
	public SparkSession sparkSession() throws Exception {
		return SparkSession.builder().appName(appName).config(sparkConf().set("spark.some.config.option", "some-value"))
				.getOrCreate();
	}

//	@Bean
//	@ConditionalOnMissingBean
//	public HiveContext hiveContext() throws Exception {
//		return new HiveContext(javaSparkContext());
//	}
}
