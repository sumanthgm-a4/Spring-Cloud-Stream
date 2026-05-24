package com.sum.spring_cloud_stream.sec04;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

public class SectionRunner {

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class Consumer {
		public static void main(String[] args) {
			SpringApplication.run(
				Consumer.class,
				"--section=sec04",
				"--config=01-consumer"
			);
		}
	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.producer")
	static class Producer {
		public static void main(String[] args) {
			SpringApplication.run(
				Producer.class,
				"--section=sec04",
				"--config=02-producer"
			);
		}
	}

}
