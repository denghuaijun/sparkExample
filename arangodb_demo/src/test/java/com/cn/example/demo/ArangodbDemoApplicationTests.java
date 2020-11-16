package com.cn.example.demo;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.arangodb.ArangoCursor;
import com.arangodb.springframework.core.ArangoOperations;
import com.arangodb.springframework.core.template.ArangoTemplate;
import com.arangodb.util.MapBuilder;
import com.cn.example.demo.entity.Test1;
import com.cn.example.demo.repository.Test1Repository;


@SpringBootTest
public class ArangodbDemoApplicationTests {
    
	@Autowired
	private Test1Repository repository;
	
	@Autowired
	private ArangoOperations operations;
	
	@Autowired
	private ArangoTemplate template;
	
	@Test
	public void contextLoads() {
		
	}

	
	/**
	 * 
	 * Description: 根据字段randNum，内连查询test2（30万）和test1（1200万）
	 * Title: test_Test2InnerJoinTest1
	 */
	@Test
	public void test_Test2InnerJoinTest1() {
		long startTime = System.currentTimeMillis();
		List<Map<String, Object>> list = repository.getTest2InnerJoinTest1();
		long endTime = System.currentTimeMillis();
		System.out.println("Test2InnerJoinTest1_第一次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		startTime = System.currentTimeMillis();
		list = repository.getTest2InnerJoinTest1();
		endTime = System.currentTimeMillis();
		System.out.println("Test2InnerJoinTest1_第二次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		startTime = System.currentTimeMillis();
		list = repository.getTest2InnerJoinTest1();
		endTime = System.currentTimeMillis();
		System.out.println("Test2InnerJoinTest1_第三次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		System.out.println("Test2InnerJoinTest1_查询记录数： " + list.size());
		
		
//		template.insert(values, entityClass)
		
	}
	
	/**
	 * 
	 * Description: 根据字段randNum，内连查询test3（300万）和test1（1200万）
	 * Title: test_Test3InnerJoinTest1
	 */
	@Test
	public void test_Test3InnerJoinTest1() {
		long startTime = System.currentTimeMillis();
		List<Map<String, Object>> list = repository.getTest3InnerJoinTest1();
		long endTime = System.currentTimeMillis();
		System.out.println("Test3InnerJoinTest1_第一次查询执行时间： " + (endTime - startTime) + "毫秒");
		
//		startTime = System.currentTimeMillis();
//		list = repository.getTest3InnerJoinTest1();
//		endTime = System.currentTimeMillis();
//		System.out.println("Test3InnerJoinTest1_第二次查询执行时间： " + (endTime - startTime) + "毫秒");
//		
//		startTime = System.currentTimeMillis();
//		list = repository.getTest3InnerJoinTest1();
//		endTime = System.currentTimeMillis();
//		System.out.println("Test3InnerJoinTest1_第三次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		System.out.println("Test3InnerJoinTest1_查询记录数： " + list.size());
	}
	/**
	 * 
	 * Description: 根据字段randNum，左连查询test2（30万）和test1（1200万）
	 * Title: test_Test2LeftJoinTest1
	 */
	@Test
	public void test_Test2LeftJoinTest1() {
		long startTime = System.currentTimeMillis();
		List<Map<String, Object>> list = repository.getTest2LeftJoinTest1();
		long endTime = System.currentTimeMillis();
		System.out.println("Test2LeftJoinTest1_第一次查询执行时间： " + (endTime - startTime) + "毫秒");
		
//		startTime = System.currentTimeMillis();
//		list = repository.getTest2LeftJoinTest1();
//		endTime = System.currentTimeMillis();
//		System.out.println("Test2LeftJoinTest1_第二次查询执行时间： " + (endTime - startTime) + "毫秒");
//		
//		startTime = System.currentTimeMillis();
//		list = repository.getTest2LeftJoinTest1();
//		endTime = System.currentTimeMillis();
//		System.out.println("Test2LeftJoinTest1_第三次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		System.out.println("Test2LeftJoinTest1_查询记录数： " + list.size());
	}
	/**
	 * 
	 * Description: 根据字段randNum，左连查询test3（300万）和test1（1200万）
	 * Title: test_Test3LeftJoinTest1
	 */
//	@Test
	public void test_Test3LeftJoinTest1() {
		long startTime = System.currentTimeMillis();
		List<Map<String, Object>> list = repository.getTest3LeftJoinTest1();
		long endTime = System.currentTimeMillis();
		System.out.println("Test3LeftJoinTest1_第一次查询执行时间： " + (endTime - startTime) + "毫秒");
		
//		startTime = System.currentTimeMillis();
//		list = repository.getTest3LeftJoinTest1();
//		endTime = System.currentTimeMillis();
//		System.out.println("Test3LeftJoinTest1_第二次查询执行时间： " + (endTime - startTime) + "毫秒");
//		
//		startTime = System.currentTimeMillis();
//		list = repository.getTest2LeftJoinTest1();
//		endTime = System.currentTimeMillis();
//		System.out.println("Test3LeftJoinTest1_第三次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		System.out.println("Test3LeftJoinTest1_查询记录数： " + list.size());
	}
	
	
	@Test
	public void testArangoOperations() {
		String query = "FOR t2 IN test2 FILTER t2.randNum == @randNum SORT t2.randName DESC RETURN t2";
		Map<String, Object> params = new MapBuilder()
				.put("randNum", "45042619760508566X")
//				.put("age", 25)
				.get();
		long startTime = System.currentTimeMillis();
		ArangoCursor<Test1> cursor = operations.query(query, params, null, Test1.class);
		long endTime = System.currentTimeMillis();
		System.out.println("testArangoOperations_第一次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		cursor.forEachRemaining(test -> {
			System.out.println("test: " + test);
		});
		
//		query = "FOR t2 IN test2 FOR t1 IN test1 FILTER t1.randNum == t2.randNum RETURN t2";
//		
//		cursor = operations.query(query, null, null, Test1.class);
//		cursor.forEachRemaining(test -> {
//			System.out.println("test: " + test);
//		});
	}
	
	@Test
	public void testArangoTemplate() {
		String query = "FOR t2 IN test2 FILTER t2.randNum == @randNum SORT t2.randName DESC RETURN t2";
		Map<String, Object> params = new MapBuilder()
				.put("randNum", "45042619760508566X")
//				.put("age", 25)
				.get();
		long startTime = System.currentTimeMillis();
		ArangoCursor<Test1> cursor = template.query(query, params, null, Test1.class);
		long endTime = System.currentTimeMillis();
		System.out.println("testArangoOperations_第一次查询执行时间： " + (endTime - startTime) + "毫秒");
		
		cursor.forEachRemaining(test -> {
			System.out.println("test: " + test);
		});
		
	}
}
