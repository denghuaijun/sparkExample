/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.entity;


import java.io.Serializable;

import lombok.Data;
@Data
public class Test2 implements Serializable{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -3954789049467825712L;

	private Integer id;
	
	private String randName;
	
	private String randNum;
}
