/**
 * @Title: Worker1.java
 * @Package com.rabbitmq.consumer
 * @Description: TODO
 * Copyright: Copyright (c) 2016 
 * Company:AutoHome
 * 
 * @author Comsys-Administrator
 * @date 2016年11月2日 下午3:48:08
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
public class Worker1 {
	private static final String TASK_QUEUE_NAME = "task_queue";
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
		final Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
		System.out.println("Worker1 [*] Waiting for messages. To exit press CTRL+C");
		//每次从队列中获取数量
		channel.basicQos(1);
		//创建队列消费者
		QueueingConsumer consumer = new QueueingConsumer(channel);
		//不启用自动应答
		boolean ack = false;
		//指定消息队列
		channel.basicConsume(TASK_QUEUE_NAME, ack, consumer);
		QueueingConsumer.Delivery delivery = null;
		while (true) {
			try {
				//nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
				delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				System.out.println("Recived" + message +"");
				//处理ok响应应答
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				doWork(message);
			} catch (Exception e) {
				if (null != delivery) {
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
				}
			}
		}
	}
	
	public static void doWork(String task) {
		try {
			Thread.sleep(500);
		} catch (Exception e) {
			Thread.currentThread().interrupt();
		}
	}
}
