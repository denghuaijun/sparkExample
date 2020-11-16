package com.cn.example.demo;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.cn.example.demo.entity.Test1;
import com.cn.example.demo.entity.Test2;
import com.cn.example.demo.entity.TestJavaEntity;
import com.cn.example.demo.function.ArangoSparkTest;
import com.cn.example.demo.util.ArangoSparkUtil;



@SpringBootTest
public class ArangodbSparkDemoApplicationTests {
    
	@Autowired
	private ArangoSparkTest  arangoSparkTest;
	
	@Autowired
	private ArangoSparkUtil  arangoSparkUtil;
	
	@Test
	public void contextLoads() {
		
	}
	
//	@Test
	void arangoSparkUtil() {
		arangoSparkUtil.loadArangodbToSparkToArangodb(new String[]{"test_col"}, new Class<?>[]{Test1.class,Test2.class},
				"SELECT * FROM test_col where id == 1", "test_coll", Test1.class);
	}
	
	@Test
	void loadArangodbToSparkToArangodb2() {
		arangoSparkTest.loadArangodbToSparkToArangodb1("test_col", Test1.class,"test_coll","id == 1");
	}
	
	@Test
	void loadArangodbToSparkToArangodb() {
		arangoSparkTest.loadArangodbToSparkToArangodb1("spark_test_col", TestJavaEntity.class,"spark_test_coll","test == 11111111");
	}
	
	@Test
	void loadArangodbToSpark() {
		arangoSparkTest.loadArangodbToSpark("spark_test_col", TestJavaEntity.class);
	}

	
	@Test
	void loadSparkToArangodb() {
		List<TestJavaEntity> docs = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			docs.add(new TestJavaEntity());
		}
		arangoSparkTest.loadSparkToArangodb("spark_test_col", docs);
	}
}
