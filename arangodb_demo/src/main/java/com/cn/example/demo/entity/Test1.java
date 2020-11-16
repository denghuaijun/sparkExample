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
@Document("test1")
public class Test1 implements Serializable{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 9027151729930090841L;

	private String id;
	
	private String randName;
	
	private String randNum;
}
