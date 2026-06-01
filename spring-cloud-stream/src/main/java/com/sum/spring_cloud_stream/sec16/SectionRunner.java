package com.sum.spring_cloud_stream.sec16;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SectionRunner {

	static void main() {
		SpringApplication.run(SectionRunner.class, "--section=sec16", "--config=04-dlq");
	}

}