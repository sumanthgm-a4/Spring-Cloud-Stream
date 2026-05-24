package com.sum.spring_cloud_stream.sec07;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

public class SectionRunner {

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class Consumer {
		static void main() {
			SpringApplication.run(
				Consumer.class,
				"--section=sec07", 
				"--config=01-consumer",
				"--server-port=8080"
			);
		}
	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.processor")
	static class Processor {
		static void main() {
			SpringApplication.run(
				Processor.class, 
				"--section=sec07", 
				"--config=05-notification-processor",
				"--server.port=8081"
			);
		}
	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.producer")
	static class Producer {
		public static void main(String[] args) {
			SpringApplication.run(
				Producer.class,
				"--section=sec07",
				"--config=02-producer",
				"--server.port=9090"
			);
		}
	}

}
