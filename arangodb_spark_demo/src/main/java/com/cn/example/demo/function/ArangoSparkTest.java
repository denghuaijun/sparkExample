/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.function;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.arangodb.spark.ArangoSpark;
import com.arangodb.spark.ReadOptions;
import com.arangodb.spark.WriteOptions;
import com.arangodb.spark.rdd.api.java.ArangoJavaRDD;
import com.cn.example.demo.util.SparkDatasetToJavaBean;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ArangoSparkTest {

	@Autowired
	private ReadOptions readOptions;
	
	@Autowired
	private WriteOptions writeOptions;
	
	@Autowired
	private JavaSparkContext sc;
	
	@Autowired
	private SparkSession spark;
	
	
	/**
	 * 
	 * Description: 关闭SparkSession
	 * Title: destructionSparkSession
	 */
	public void destructionSparkSession() {
		spark.stop();
	}
	
	/**
	 * 
	 * Description: 关闭JavaSparkContext
	 * Title: destructionJavaSparkContext
	 */
	public void destructionJavaSparkContext() {
		sc.stop();
	}
	
	/**
	 * 
	 * Description: 读取arangodb数据到spark，通过spark sql查询结果 再把结果写入新的集合
	 * Title: loadArangodbToSparkToArangodb1
	 * @param <T>
	 * @param queryCollection  查询集合 
	 * @param querycollectionClass
	 * @param writeCollection 写入的集合 
	 */
	public <T> void loadArangodbToSparkToArangodb1(String queryCollection,Class<T> querycollectionClass,String writeCollection,String sqlConditions) {
		long start = System.currentTimeMillis();
		ArangoJavaRDD<T> rdd = ArangoSpark.load(sc, queryCollection, readOptions,
				querycollectionClass);
		long end = System.currentTimeMillis();
		log.info("☆☆☆☆☆☆☆ spark读取arangodb数据 ☆☆☆☆☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒",queryCollection,0,(end - start));
		Dataset<Row> dataset = spark.createDataFrame(rdd, querycollectionClass);
		dataset.createOrReplaceTempView(writeCollection);
		dataset = spark.sql("SELECT * FROM " + writeCollection + (StringUtils.isEmpty(sqlConditions) ? "" : " where " + sqlConditions));
		dataset.show(10);
		List<T> jsonArray = SparkDatasetToJavaBean.getStreamToJavaBean(dataset, querycollectionClass);
		System.out.println(jsonArray.get(0).toString());
		JavaRDD<T> documents = sc.parallelize(jsonArray);
		start = System.currentTimeMillis();
		ArangoSpark.save(documents, writeCollection, writeOptions);
		
		end = System.currentTimeMillis();
		log.info("☆☆☆☆☆☆☆ spark写入arangodb数据 ☆☆☆☆☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒",writeCollection,documents.count(),(end - start));
		destructionSparkSession();
		destructionJavaSparkContext();
	}
	
	
	public <T> void loadArangodbToSparkToArangodb(String queryCollection,Class<T> querycollectionClass,String writeCollection) {
		long start = System.currentTimeMillis();
		ArangoJavaRDD<T> rdd = ArangoSpark.load(sc, queryCollection, readOptions,
				querycollectionClass);
		long end = System.currentTimeMillis();
		log.info("☆☆☆☆☆☆☆ spark读取arangodb数据 ☆☆☆☆☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒",queryCollection,rdd.count(),(end - start));
		start = System.currentTimeMillis();
		ArangoSpark.save(rdd, writeCollection, writeOptions);
		end = System.currentTimeMillis();
		log.info("☆☆☆☆☆☆☆ spark写入arangodb数据 ☆☆☆☆☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒",writeCollection,rdd.count(),(end - start));
		destructionJavaSparkContext();
	}
	
	/**
	 * 
	 * Description: spark读取arangodb数据
	 * Title: loadAll 
	 * @param <T>
	 * @param collection  集合名
	 * @param collectionClass  集合对象
	 */
	public <T> void loadArangodbToSpark(String collection,Class<T> collectionClass) {
		long start = System.currentTimeMillis();
		ArangoJavaRDD<T> rdd = ArangoSpark.load(sc, collection, readOptions,
				collectionClass);
		long end = System.currentTimeMillis();
		log.info("☆☆☆☆☆☆☆ spark读取arangodb数据 ☆☆☆☆☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒",collection,rdd.count(),(end - start));
		destructionJavaSparkContext();
	}
	
	/**
	 * 
	 * Description: spark写入arangodb数据
	 * Title: loadSparkToArangodb
	 * @param <T>
	 * @param collection  集合名
	 * @param docs 集合数据
	 */
	public <T> void loadSparkToArangodb(String collection,List<T> docs) {
		long start = System.currentTimeMillis();
		JavaRDD<T> documents = sc.parallelize(docs);
		ArangoSpark.save(documents, collection, writeOptions);
		long end = System.currentTimeMillis();
		log.info("☆☆☆☆☆☆☆ spark写入arangodb数据 ☆☆☆☆☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒",collection,documents.count(),(end - start));
		destructionJavaSparkContext();
	}
}
