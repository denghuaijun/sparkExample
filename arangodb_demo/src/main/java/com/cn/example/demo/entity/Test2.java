/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.entity;


import java.io.Serializable;

import com.arangodb.springframework.annotation.Document;

import lombok.Data;
@Data
@Document("test2")
public class Test2 implements Serializable{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -3816296403603033159L;

	private String id;
	
	private String randName;
	
	private String randNum;
}
