/*
 * Company: 
 * Copyright (c) 2012-2032 
 * All Rights Reserved.
 */
package com.cn.example.demo.repository;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.springframework.data.repository.NoRepositoryBean;

import com.arangodb.springframework.annotation.Query;
import com.arangodb.springframework.repository.ArangoRepository;


@NoRepositoryBean
public interface BaseRepository<T, ID extends Serializable> extends ArangoRepository<T, ID> {

	/**
     * 
     * Description: test1/test2 inner join
     * Title: getTest2InnerJoinTest1
     * @return
     */
    @Query("FOR t2 IN test2 FOR t1 IN test1 FILTER t1.randNum == t2.randNum RETURN { name: t2.randName, idCard: t1.randNum }")
    List<Map<String, Object>> getTest2InnerJoinTest1();
    
    /**
     * 
     * Description: test1/test3 inner join
     * Title: getTest3InnerJoinTest1
     * @return
     */
    @Query("FOR t3 IN test3 FOR t1 IN test1 FILTER t1.randNum == t3.randNum RETURN { name: t3.randName, idCard: t1.randNum }")
    List<Map<String, Object>> getTest3InnerJoinTest1();
    
    /**
     * 
     * Description: test1/test2 left  join
     * Title: getTest2LeftJoinTest1
     * @return
     */
    @Query("FOR t2 IN test2 LET t = (FOR t1 IN test1 FILTER t1.randNum == t2.randNum RETURN {idCard: t1.randNum}) RETURN {name: t2.randName,idCard: t2.randNum,idCards: TO_STRING(t)}")
    List<Map<String, Object>> getTest2LeftJoinTest1();
    
    /**
     * 
     * Description: test1/test3 left  join
     * Title: getTest3LeftJoinTest1
     * @return
     */
    @Query("FOR t3 IN test3 LET t = (FOR t1 IN test1 FILTER t1.randNum == t3.randNum RETURN {idCard: t1.randNum}) RETURN {name: t3.randName,idCard: t3.randNum,idCards: TO_STRING(t)}")
    List<Map<String, Object>> getTest3LeftJoinTest1();
}
