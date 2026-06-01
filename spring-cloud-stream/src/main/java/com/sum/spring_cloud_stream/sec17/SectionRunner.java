package com.sum.spring_cloud_stream.sec17;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

public class SectionRunner {

	@EnableScheduling
	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class Consumer {

		static void main() {
			SpringApplication.run(
					Consumer.class, "--section=sec17", "--config=01-consumer"
			);
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.producer")
	static class Producer {

		static void main() {
			SpringApplication.run(
					Producer.class, "--section=sec17", "--config=02-producer", "--server.port=9090"
			);
		}

	}

}