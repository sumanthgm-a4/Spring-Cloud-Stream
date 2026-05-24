package com.sum.spring_cloud_stream.sec06;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

public class SectionRunner {

	// Multiple (3) consumers

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class Consumer1 {
		public static void main(String[] args) {
			SpringApplication.run(
				Consumer1.class,
				"--section=sec06",
				"--config=01-consumer",
				"--server.port=8080"
			);
		}
	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class Consumer2 {
		public static void main(String[] args) {
			SpringApplication.run(
				Consumer2.class,
				"--section=sec06",
				"--config=01-consumer",
				"--server.port=8081"
			);
		}
	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class Consumer3 {
		public static void main(String[] args) {
			SpringApplication.run(
				Consumer3.class,
				"--section=sec06",
				"--config=01-consumer",
				"--server.port=8082"
			);
		}
	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.producer")
	static class Producer {
		public static void main(String[] args) {
			SpringApplication.run(
				Producer.class,
				"--section=sec06",
				"--config=02-producer"
			);
		}
	}

}
