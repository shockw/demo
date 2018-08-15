/**
 * @Title: KafkaConsumerRunner.java
 * @Package com.ustcinfo.ishare.eip.consumer
 * @Description: 
 * Copyright:© 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights reserved.
 * 
 * @author Ni.Liang
 * @date 2017年3月29日 下午5:50:05
 * @version V1.0
 */
package com.ustcinfo.ishare.bdp.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * @describe 自动提交offset的kafka多线程消费，当线程数大于分区（partition）数时，多出的线程会等待，如果线程数小于分区（
 *           partition）时， 一个线程会消费多个分区
 * @author Nee
 * @mail ni.liang@ustcinfo.com
 * @date 2017年4月13日上午10:57:23
 * @copyright © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 *            reserved.
 *
 */
public class AutoCommitOffsetThreadConsumerRunner implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private KafkaConsumer<String, String> consumer;

	public void run() {
		try {
			consumer = new KafkaConsumer<>(getProperties());
			consumer.subscribe(Arrays.asList("test"));
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(10000);
				for (ConsumerRecord<String, String> record : records)
					System.out.println(Thread.currentThread().getName() + " : " + record.toString());
			}
		} catch (WakeupException e) {
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		if (consumer != null) {
			closed.set(true);
			consumer.wakeup();
		}
	}

	public static Properties getProperties() {
		Properties props = new Properties();
		/** broker集群地址 */
		props.put("bootstrap.servers", "node101:1132,node86:1132");
		/** 消费组名称 */
		props.put("group.id", "test1");
		/** 设置是否自动提交offset */
		props.put("enable.auto.commit", "true");
		/**
		 * 当前consumer所在的group获取上次读到的offset不存在将执行的动作，earliest:设置offset到0 latest:
		 * 设置offset到最后, none:抛出异常
		 */
		props.put("auto.offset.reset", "earliest");
		/** 设置自动提交时间 */
		props.put("auto.commit.interval.ms", "1000");
		/** key的序列化类 */
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		/** value的序列化类 */
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	public static void main(String[] args) {
		Thread thread = new Thread(new AutoCommitOffsetThreadConsumerRunner(), "nee-0");
		Thread thread2 = new Thread(new AutoCommitOffsetThreadConsumerRunner(), "nee-1");
		Thread thread3 = new Thread(new AutoCommitOffsetThreadConsumerRunner(), "nee-2");
		Thread thread4 = new Thread(new AutoCommitOffsetThreadConsumerRunner(), "nee-3");
		thread.start();
		thread2.start();
		thread3.start();
		thread4.start();
	}
}
