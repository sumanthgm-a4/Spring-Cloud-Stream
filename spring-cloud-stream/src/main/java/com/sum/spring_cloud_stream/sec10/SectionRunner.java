package com.sum.spring_cloud_stream.sec10;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

public class SectionRunner {

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class DigitalDeliveryConsumer {

		static void main() {
			SpringApplication.run(DigitalDeliveryConsumer.class, "--section=sec10", "--config=01-digital-consumer", "--server.port=8080");
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class FedExConsumer {

		static void main() {
			SpringApplication.run(FedExConsumer.class, "--section=sec10", "--config=02-fedex-consumer", "--server.port=8081");
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.consumer")
	static class USPSConsumer {

		static void main() {
			SpringApplication.run(USPSConsumer.class, "--section=sec10", "--config=03-usps-consumer", "--server.port=8082");
		}

	}

	@EnableScheduling
	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.processor")
	static class Processor {

		static void main() {
			SpringApplication.run(Processor.class, "--section=sec10", "--config=04-processor","--server.port=8083");
		}

	}

	@SpringBootApplication(scanBasePackages = "com.sum.spring_cloud_stream.${section}.producer")
	static class Producer {

		static void main() {
			SpringApplication.run(Producer.class, "--section=sec10", "--config=05-producer", "--server.port=8084");
		}

	}

}