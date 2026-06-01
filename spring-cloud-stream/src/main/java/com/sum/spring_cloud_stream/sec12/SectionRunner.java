package com.sum.spring_cloud_stream.sec12;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


public class SectionRunner {

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class Consumer {

		static void main() {
			SpringApplication.run(
					Consumer.class, "--section=sec12", "--config=01-consumer", "--server.port=8080"
			);
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.producer")
	static class Producer {

		static void main() {
			SpringApplication.run(
					Producer.class, "--section=sec12", "--config=02-producer", "--server.port=9090"
			);
		}

	}

}