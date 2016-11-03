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
public class ReceiveLogsTopic2 {
	private static final String[] routingKeys = new String[]{"mayun.#"};
	private static final String EXCHANGE_NAME = "topic2_logs";
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
		//声明交换器
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		//每次从队列中获取数量
		channel.basicQos(1);
		//创建队列消费者
		QueueingConsumer consumer = new QueueingConsumer(channel);
		//不启用自动应答
		boolean ack = false;
		//获取匿名队列名称
		String queueName = channel.queueDeclare().getQueue();
		//指定消息队列
		channel.basicConsume(queueName, ack, consumer);
		//根据路由关键字进行多重绑定
		for (String bindingKey : routingKeys) {
			channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
			System.out.println("ReciveLogsTopic1 exchange：" + EXCHANGE_NAME +",queue:" +queueName+"BindRoutingKey:" +bindingKey);
		}
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		QueueingConsumer.Delivery delivery = null;
		while (true) {
			try {
				//nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
				delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody(), "UTF-8");
				System.out.println("ReciveLogsTopic1 Recived:" + message +"");
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
