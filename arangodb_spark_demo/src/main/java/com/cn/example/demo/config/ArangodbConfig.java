/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.arangodb.Protocol;
import com.arangodb.spark.ReadOptions;
import com.arangodb.spark.WriteOptions;

@Configuration
public class ArangodbConfig {

	@Value("${arangodb.hosts}")
	private String hosts;

	@Value("${arangodb.user}")
    private String user;
	
	@Value("${arangodb.password}")
    private String password;
	
	@Value("${arangodb.database}")
    private String database;
	
	@Value("${arangodb.protocol}")
    private Protocol protocol;
	
	@Value("${arangodb.useSsl}")
    private boolean useSsl;
	
	@Bean
	public ReadOptions readOptions() {
		ReadOptions readOptions = new ReadOptions().hosts(hosts).database(database)
				.user(user).password(password).useSsl(useSsl).protocol(protocol);
		return readOptions;
	}
	
	@Bean
	public WriteOptions writeOptions() {
		WriteOptions writeOptions = new WriteOptions().hosts(hosts).database(database)
				.password(password).user(user).useSsl(useSsl).protocol(protocol);
		return writeOptions;
	}
}
