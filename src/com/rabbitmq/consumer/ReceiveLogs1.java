/**
 * @Title: ReceiveLogs1.java
 * @Package com.rabbitmq.consumer
 * @Description: TODO
 * Copyright: Copyright (c) 2016 
 * Company:AutoHome
 * 
 * @author Comsys-Administrator
 * @date 2016年11月3日 下午3:54:09
 * @version V1.0
 */
package com.rabbitmq.consumer;

import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * @author Administrator
 *
 */
public class ReceiveLogs1 {
	private static String EXCHANGE_NAME = "logs";
	private static final String HOST = "10.168.11.166";
	private static final String USERNAME = "admin";
	private static final String PASSWORD = "admin1234";
	private static final int PORT = 5672;
	public static void main(String[] args) throws Exception, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		factory.setPort(PORT);
		factory.setUsername(USERNAME);
		factory.setPassword(PASSWORD);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		//每次从队列中获取数量
		channel.basicQos(1);
		//创建队列消费者
		QueueingConsumer consumer = new QueueingConsumer(channel);
		//不启用自动应答
		boolean ack = false;
		String queueName = channel.queueDeclare().getQueue();
		channel.queueBind(queueName, EXCHANGE_NAME, "");
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		//指定消息队列
		channel.basicConsume(queueName, ack, consumer);
		QueueingConsumer.Delivery delivery = null;
		while (true) {
			try {
				//nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
				delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				System.out.println("ReceiveLogs1 Recived:" + message +"");
				//处理ok响应应答
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			} catch (Exception e) {
				if (null != delivery) {
						channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
				}
			}
		}
	}
}
