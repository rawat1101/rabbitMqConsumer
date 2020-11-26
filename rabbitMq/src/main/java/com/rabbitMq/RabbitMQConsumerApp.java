package com.rabbitMq;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.rabbitMq.RabbitMQConsumerApp.RabbitMqThreadFactory;
import com.rabbitMq.RabbitMqConfigs.RabbitMqConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

@SpringBootApplication
public class RabbitMQConsumerApp {
//	private AtomicInteger ai = new AtomicInteger();

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(RabbitMQConsumerApp.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.setLogStartupInfo(false);
		app.run(args);
		System.out.println("====================== MAIN =========================");
	}

	
	static class RabbitMqThreadFactory implements ThreadFactory {
		private final AtomicInteger threadNumber = new AtomicInteger(1);
		private final String namePrefix;

		RabbitMqThreadFactory() {
			namePrefix = "rabbit-" + "-thread-";
		}

		public Thread newThread(Runnable r) {
			return new Thread(r, namePrefix + threadNumber.getAndIncrement());
		}
	}
}

@RestController
class MessageRestController {
	@Autowired
	private RabbitMqConsumer con;

	@RequestMapping("/message")
	int getMessage() {
		return con.getAi().get();
	}
}

@Component
@ConfigurationProperties(prefix = "rabbit-mq")
class Credentials {
	String host;
	int port;
	String user;
	String password;
	String queue;
	String exchange;
	String exchangeType;
	String routingKey;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getExchangeType() {
		return exchangeType;
	}

	public void setExchangeType(String exchangeType) {
		this.exchangeType = exchangeType;
	}

	public String getRoutingKey() {
		return routingKey;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

}

@Configuration
class RabbitMqConfigs {
	@Autowired
	Credentials credentials;

	@Bean
	public Channel rabbitMqChannel() throws IOException, TimeoutException {
		Connection connection = rabbitMqConnection();
		return connection.createChannel();

	}

	public Connection rabbitMqConnection() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(credentials.getHost());
		factory.setPort(credentials.getPort());
		factory.setUsername(credentials.getUser());
		factory.setPassword(credentials.getPassword());
		factory.setAutomaticRecoveryEnabled(true);
		factory.setNetworkRecoveryInterval(10000);
		final ExecutorService tp = Executors.newFixedThreadPool(2, new RabbitMqThreadFactory());
		return factory.newConnection(tp);

	}

	@Component
	class RabbitMqConsumer implements CommandLineRunner {
		@Autowired
		private Channel channel;
		@Autowired
		private Credentials credentials;
		private AtomicInteger ai = new AtomicInteger();

		public AtomicInteger getAi() {
			return ai;
		}

		@Override
		public void run(String... args) throws Exception {

			try {
				System.out.println("#############################################################");
				channel.queueDeclare(credentials.getQueue(), true, false, false, null);
				channel.exchangeDeclare(credentials.getExchange(), credentials.getExchangeType(), false, false, null);
				channel.queueBind(credentials.getQueue(), credentials.getExchange(), credentials.getRoutingKey());
//			channel.basicQos(100);
				final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
					/*String mssg = new String(delivery.getBody(), "UTF-8");
					mssg += "Received from message from the queue: " + Thread.currentThread().getName() + "  " + mssg;
					System.out.println(mssg);*/
					ai.incrementAndGet();
					/*int count = ai.addAndGet(1);
					if (count % 100 == 0)
						channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);*/
				};

				channel.basicConsume(credentials.getQueue(), false, deliverCallback, consumerTag -> {
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}
