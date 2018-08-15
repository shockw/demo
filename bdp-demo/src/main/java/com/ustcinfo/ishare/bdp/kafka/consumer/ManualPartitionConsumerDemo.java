/**
 * @Title: ManualPartitionConsumer.java
 * @Package com.ustcinfo.ishare.eip.consumer
 * @Description: 
 * Copyright:© 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights reserved.
 * 
 * @author Ni.Liang
 * @date 2017年4月11日 上午10:03:24
 * @version V1.0
 */
package com.ustcinfo.ishare.bdp.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * @describe 消费指定partition的demo
 * @author Nee
 * @mail ni.liang@ustcinfo.com
 * @date 2017年4月11日上午10:03:24
 * @copyright © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 *            reserved.
 *
 */
public class ManualPartitionConsumerDemo {
	public static void send() {
		Properties props = new Properties();
		/** broker集群地址 ,可以只写一个或一部分 */
		props.put("bootstrap.servers", "node101:1132,node86:1132");
		/** groupId */
		props.put("group.id", "nltest2");
		/**
		 * 当前consumer给出的offset不存在将执行的动作，earliest:设置offset到0 latest:设置offset到最后,
		 * none:抛出异常
		 */
		props.put("auto.offset.reset", "earliest");
		/** 设置是否自动提交偏移量 */
		props.put("enable.auto.commit", "true");
		/** 偏移量提交的频率 */
		props.put("auto.commit.interval.ms", "1000");
		/** 设置心跳时间 */
		props.put("session.timeout.ms", "30000");
		/** key的序列化类 */
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		/** value的序列化类 */
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		TopicPartition partition0 = new TopicPartition("test", 1);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.assign(Arrays.asList(partition0));
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records)
					System.out.printf("offset = %d, key = %s, value = %s,partition= %s  \r\n", record.offset(),
							record.key(), record.value(), record.partition());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (WakeupException e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	public static void main(String[] args) {
		send();
	}
}
