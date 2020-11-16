/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.config;

import com.arangodb.ArangoDB;

//import org.springframework.context.annotation.Configuration;

import com.arangodb.ArangoDB.Builder;
import com.arangodb.Protocol;
//import com.arangodb.springframework.annotation.EnableArangoRepositories;
import com.arangodb.springframework.config.AbstractArangoConfiguration;
/**
 * 
 * Description: springboot 支持ArangoDB配置自动装配
 * @author LiuHao
 * @date 2020年5月15日下午3:39:55
 * @version 1.0
 */
//@Configuration
//@EnableArangoRepositories(basePackages = { "com.cn.example.demo" })
@SuppressWarnings("deprecation")
public class ArangoDBConfiguration extends AbstractArangoConfiguration  {

	@Override
	public Builder arango() {
		ArangoDB.Builder arango = new ArangoDB.Builder()
                .useProtocol(Protocol.HTTP_JSON)
                .host("10.200.1.183", 8529)
                .user("root")
                .password("root");
        return arango;
	}

	@Override
	public String database() {
		// TODO Auto-generated method stub
		return "_system";
	}

}