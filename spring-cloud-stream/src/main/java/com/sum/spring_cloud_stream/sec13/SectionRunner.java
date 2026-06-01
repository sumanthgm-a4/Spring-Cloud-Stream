package com.sum.spring_cloud_stream.sec13;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

public class SectionRunner {

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class DigitalDeliveryConsumer {

		static void main() {
			SpringApplication.run(DigitalDeliveryConsumer.class, "--section=sec13", "--config=01-digital-consumer");
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class PhysicalDeliveryConsumer {

		static void main() {
			SpringApplication.run(PhysicalDeliveryConsumer.class, "--section=sec13", "--config=02-physical-consumer", "--server.port=8081");
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.processor")
	static class Processor {

		static void main() {
			SpringApplication.run(Processor.class, "--section=sec13", "--config=03-processor", "--server.port=8082");
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.producer")
	static class Producer {

		static void main() {
			SpringApplication.run(Producer.class, "--section=sec13", "--config=04-producer", "--server.port=8083");
		}

	}

}