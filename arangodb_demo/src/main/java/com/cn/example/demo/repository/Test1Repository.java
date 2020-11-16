/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.repository;


import com.arangodb.springframework.annotation.Query;
import com.cn.example.demo.entity.Test1;

public interface Test1Repository extends BaseRepository<Test1, String> {

	Iterable<Test1> findByRandName(String randName);

    @Query("FOR t1 IN test1 FILTER t1.randNum > @randNum SORT t1.randName DESC RETURN t1")
    Iterable<Test1> getName(String randNum);
    
}
