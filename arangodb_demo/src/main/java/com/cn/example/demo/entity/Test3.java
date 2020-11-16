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
@Document("test3")
public class Test3 implements Serializable{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 3775265572138928229L;

	private String id;
	
	private String randName;
	
	private String randNum;
}
