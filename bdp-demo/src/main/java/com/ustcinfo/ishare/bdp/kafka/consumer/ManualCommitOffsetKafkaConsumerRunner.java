/**
 * @Title: ManualCommitConsumerDemo.java
 * @Package com.ustcinfo.ishare.eip.consumer
 * @Description: 
 * Copyright:© 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights reserved.
 * 
 * @author Ni.Liang
 * @date 2017年4月11日 上午9:44:13
 * @version V1.0
 */
package com.ustcinfo.ishare.bdp.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import com.ustcinfo.ishare.bdp.kafka.callback.MyOffsetCommitCallback;


/**
 * @describe 手动提交offset的demo
 * @author Nee
 * @mail ni.liang@ustcinfo.com
 * @date 2017年4月11日上午9:44:13
 * @copyright © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 *            reserved.
 *
 */
public class ManualCommitOffsetKafkaConsumerRunner implements Runnable {

	public static Properties getProperties() {
		Properties props = new Properties();
		/** broker集群地址 ,可以只写一个或一部分 */
		props.put("bootstrap.servers", "node101:1132,node86:1132");
		/** groupId */
		props.put("group.id", "nltest1");
		/**
		 * 当前consumer给出的offset不存在将执行的动作，earliest:设置offset到0 latest:设置offset到最后,
		 * none:抛出异常
		 */
		props.put("auto.offset.reset", "earliest");
		/** 设置是否自动提交偏移量 */
		props.put("enable.auto.commit", "false");
		/** 设置心跳时间 */
		props.put("session.timeout.ms", "30000");
		/** key的序列化类 */
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		/** value的序列化类 */
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	@Override
	public void run() {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getProperties());
		consumer.subscribe(Arrays.asList("test"));
		final int minBatchSize = 20; // 批量提交数量
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(Thread.currentThread().getName() + " : consumer message values is "
							+ record.value() + " and the offset is " + record.offset() + " and partition is "
							+ record.partition());
					buffer.add(record);
					if (buffer.size() >= minBatchSize) {
						System.out.println("offset is " + record.offset());
						buffer.clear();
					}
					Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
					TopicPartition partition = new TopicPartition("test", record.partition());
					OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
					offsets.put(partition, offsetAndMetadata);
					consumer.commitAsync(offsets, new MyOffsetCommitCallback());// (new
				}
			}
		} catch (WakeupException e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	public static void main(String[] args) {
		Thread thread1 = new Thread(new ManualCommitOffsetKafkaConsumerRunner(), "0");
		Thread thread2 = new Thread(new ManualCommitOffsetKafkaConsumerRunner(), "1");
		Thread thread3 = new Thread(new ManualCommitOffsetKafkaConsumerRunner(), "2");
		Thread thread4 = new Thread(new ManualCommitOffsetKafkaConsumerRunner(), "3");
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
	}

}
