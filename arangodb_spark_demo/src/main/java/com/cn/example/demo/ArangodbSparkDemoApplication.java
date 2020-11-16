package com.cn.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.cn.example.demo.entity.Test1;
import com.cn.example.demo.entity.Test2;
import com.cn.example.demo.util.ArangoSparkUtil;

@SpringBootApplication
public class ArangodbSparkDemoApplication implements CommandLineRunner {
	
	@Autowired
	private ArangoSparkUtil  arangoSparkUtil;
	
	public static void main(String[] args) {
		SpringApplication.run(ArangodbSparkDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		this.computingTasks();
	}
	
	public void computingTasks() {
		arangoSparkUtil.loadArangodbToSparkToArangodb(new String[]{"test_col"}, 
		new Class<?>[]{Test1.class},
		"SELECT * FROM test_col", "test_coll", Test2.class);
	}
}
