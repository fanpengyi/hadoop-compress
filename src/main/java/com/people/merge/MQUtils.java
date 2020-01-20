package com.people.merge;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MQUtils {
	
	private static final Logger LOG = LoggerFactory.getLogger(MQUtils.class);
			
	
	
	
	public static List<String> readMQ(String mq_url, String exchange, String queueName, String routkey, int limit){
		List<String> mqList = new ArrayList<String>();
		int count = 0;
		Connection mqConn = null;
		Channel ch = null;
		try {
			ConnectionFactory connFactory = new ConnectionFactory();
			connFactory.setUri(mq_url);
			mqConn = connFactory.newConnection();
			ch = mqConn.createChannel();
			ch.exchangeDeclare(exchange, "topic");
			ch.queueDeclare(queueName, false, false, false, null);
			ch.queueBind(queueName, exchange, routkey);
			QueueingConsumer consumer = new QueueingConsumer(ch);
			ch.basicConsume(queueName, consumer);
			
			while(true){
				if(count>limit-1){
					break;
				}
				QueueingConsumer.Delivery delivery = consumer.nextDelivery(5000);
				if(delivery != null){
					Object obj = SerializeUtil.unserialize(delivery.getBody());
					if(obj instanceof String){
						mqList.add((String)obj);
						count++;
					}
					ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				}else{
					break;
				}
			}
			ch.close();
			ch = null;
			mqConn.close();
			mqConn = null;
			connFactory = null;
		} catch (Exception e) {
			LOG.error("readArticleMQ error {}", e.getStackTrace()+"  MQ: "+mq_url);
		}finally {
			close(mqConn, ch);
		}
		return mqList;
	}



	public static void close(Connection connection, Channel channel){
		try {
			if(channel != null && channel.isOpen()){
				channel.close();
			}
			if(connection != null && connection.isOpen()){
				connection.close();
			}
		} catch (Exception e) {
			LOG.error("close error", e);
		}
	}
}
