/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.util;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
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

import lombok.extern.slf4j.Slf4j;
@Component
@Slf4j
public class ArangoSparkUtil {
	
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
	 * Description: 关闭SparkSession Title: destructionSparkSession
	 */
	public void destructionSparkSession() {
		if(null != spark) {
			spark.stop();
		}
	}

	/**
	 * 
	 * Description: 关闭JavaSparkContext Title: destructionJavaSparkContext
	 */
	public void destructionJavaSparkContext() {
		if(null != sc) {
			sc.stop();
		}
	}
	
	
	/**
	 * 
	 * Description: 读取arangodb数据到spark，通过spark 关联sql查询结果 再把结果写入arangodb新的集合(多集合)
	 * @param <T>
	 * @param queryCollections 查询Arangodb的集合
	 * @param querycollectionClass 查询的集合对应对象
	 * @param sqlConditions  spark关联查询sql语句
	 * @param writeCollection 写入Arangodb的集合
	 * @param writeCollectionClass 写入集合对应对象
	 */
	@Nonnull
	public <T> void loadArangodbToSparkToArangodb(String[] queryCollections, Class<?>[] querycollectionClass,
			String sqlConditions, String writeCollection,Class<T> writeCollectionClass) {
		try {
			long start = System.currentTimeMillis();
			long end = 0;
			//读取arangodb数据到spark
			long count = 0;
			for (int i = 0; i < queryCollections.length; i++) {
				start = System.currentTimeMillis();
				ArangoJavaRDD<?> rdd = ArangoSpark.load(sc, queryCollections[i],readOptions, querycollectionClass[i]);
				end = System.currentTimeMillis();
				Dataset<Row> dataset = spark.createDataFrame(rdd, querycollectionClass[i]);
				dataset.createOrReplaceTempView(queryCollections[i]);// 创建spark临时表
				log.info("☆☆☆ spark读取arangodb数据(多集合) ☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒", 
						queryCollections[i], rdd.count(), (end - start));
				count += rdd.count();
			}
			end = System.currentTimeMillis();
			log.info("☆☆☆ spark读取arangodb数据(多集合) ☆☆☆  collection:{} **** 总数据量: {} **** takeTime: {}毫秒", 
					StringUtils.join(queryCollections, ","), count, (end - start));
			
			// 处理spark查询结果 Dataset<Row> 转对象
			start = System.currentTimeMillis();
			Dataset<Row> dataset = spark.sql(sqlConditions);
			dataset.show();
			List<T> list = SparkDatasetToJavaBean.getStreamToJavaBean(dataset, writeCollectionClass);
			JavaRDD<T> documents = sc.parallelize(list);
			end = System.currentTimeMillis();
			log.info("☆☆☆ 处理spark查询结果 Dataset<Row>转对象 ☆☆☆  数据量: {} **** takeTime: {}毫秒",
					writeCollection,documents.count(),(end - start));
			
			//spark写入arangodb数据
			start = System.currentTimeMillis();
			ArangoSpark.save(documents, writeCollection, writeOptions);
			end = System.currentTimeMillis();
			log.info("☆☆☆ spark写入arangodb数据 ☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒", 
					writeCollection, documents.count(), (end - start));
		} catch (Exception e) {
			e.printStackTrace();
			log.error("读取arangodb数据到spark，通过spark 关联sql查询结果 再把结果写入arangodb新的集合(多集合) 异常！错误信息：{}", e.getMessage());
		} finally {
			destructionSparkSession();
			destructionJavaSparkContext();
		}
	}

	/**
	 * 
	 * Description: 读取arangodb数据到spark，通过spark sql查询结果 再把结果写入arangodb新的集合(单集合)
	 * @param <T>
	 * @param queryCollection 查询Arangodb的集合
	 * @param querycollectionClass
	 * @param writeCollection 写入Arangodb的集合
	 * @param sqlConditions spark查询SQL
	 */
	public <T> void loadArangodbToSparkToArangodb(String queryCollection, Class<T> querycollectionClass,
			String writeCollection, String sqlConditions) {
		try {
			//spark读取arangodb数据
			long start = System.currentTimeMillis();
			ArangoJavaRDD<?> rdd = ArangoSpark.load(sc, queryCollection,readOptions, querycollectionClass);
			long end = System.currentTimeMillis();
			log.info("☆☆☆ spark读取arangodb数据 ☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒", 
					queryCollection, rdd.count(), (end - start));
			
			Dataset<Row> dataset = spark.createDataFrame(rdd, querycollectionClass);
			dataset.createOrReplaceTempView(queryCollection);// 创建spark临时表
			
			// 处理spark查询结果 Dataset<Row> 转对象
			start = System.currentTimeMillis();
			dataset = spark.sql(sqlConditions);
			List<T> list = SparkDatasetToJavaBean.getStreamToJavaBean(dataset, querycollectionClass);
			JavaRDD<T> documents = sc.parallelize(list);
			end = System.currentTimeMillis();
			log.info("☆☆☆ 处理spark查询结果 Dataset<Row>转对象 ☆☆☆  数据量: {} **** takeTime: {}毫秒",
					writeCollection,documents.count(),(end - start));
			
			//spark写入arangodb数据
			start = System.currentTimeMillis();
			ArangoSpark.save(documents, writeCollection, writeOptions);
			end = System.currentTimeMillis();
			log.info("☆☆☆ spark写入arangodb数据 ☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒", 
					writeCollection, documents.count(), (end - start));
			destructionSparkSession();
			destructionJavaSparkContext();
		} catch (Exception e) {
			log.error("读取arangodb数据到spark，通过spark sql查询结果 再把结果写入arangodb新的集合(单集合) 异常！错误信息：{}", e.getMessage());
		} finally {
			destructionSparkSession();
			destructionJavaSparkContext();
		}
	}

	/**
	 * 
	 * Description: spark读取arangodb数据 并写入arangodb
	 * @param <T>
	 * @param queryCollection
	 * @param querycollectionClass
	 * @param writeCollection
	 */
	public <T> void loadArangodbToSparkToArangodb(String queryCollection, Class<T> querycollectionClass,
			String writeCollection) {
		ArangoJavaRDD<T> rdd = ArangoSpark.load(sc, queryCollection, readOptions,
				querycollectionClass);
		ArangoSpark.save(rdd, writeCollection, writeOptions);
		destructionJavaSparkContext();
	}

	/**
	 * 
	 * Description: spark读取arangodb数据 
	 * @param <T>
	 * @param collection      集合名
	 * @param collectionClass 集合对象
	 */
	public <T> void loadArangodbToSpark(String collection, Class<T> collectionClass) {
		long start = System.currentTimeMillis();
		ArangoJavaRDD<T> rdd = ArangoSpark.load(sc, collection, readOptions, collectionClass);
		long end = System.currentTimeMillis();
		log.info("☆☆☆ spark读取arangodb数据 ☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒", 
				collection, rdd.count(), (end - start));
		destructionJavaSparkContext();
	}

	/**
	 * 
	 * Description: spark写入arangodb数据 
	 * @param <T>
	 * @param collection 集合名
	 * @param docs       集合数据
	 */
	public <T> void loadSparkToArangodb(String collection, List<T> docs) {
		long start = System.currentTimeMillis();
		JavaRDD<T> documents = sc.parallelize(docs);
		ArangoSpark.save(documents, collection, writeOptions);
		long end = System.currentTimeMillis();
		log.info("☆☆☆ spark写入arangodb数据 ☆☆☆  collection:{} **** 数据量: {} **** takeTime: {}毫秒",
				collection, documents.count(), (end - start));
		destructionJavaSparkContext();
	}
}
