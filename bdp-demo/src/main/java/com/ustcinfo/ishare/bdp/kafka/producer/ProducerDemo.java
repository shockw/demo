/**
 * @Title: ProducerDemo.java
 * @Package com.ustcinfo.ishare.eip.producer
 * @Description: 
 * Copyright:© 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights reserved.
 * 
 * @author Ni.Liang
 * @date 2017年3月29日 下午4:53:32
 * @version V1.0
 */
package com.ustcinfo.ishare.bdp.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.ustcinfo.ishare.bdp.kafka.callback.MyCallBack;


/**
 * @describe 多线程生产消息,在开发之前。进入kafka客户端建立一个topic：nee。3个partition，2个replica。命令为
 *           bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic nee
 *           --partitions 3 --replication-factor 2
 * @author Nee
 * @mail ni.liang@ustcinfo.com
 * @date 2017年4月13日上午10:53:52
 * @copyright © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 *            reserved.
 *
 */
public class ProducerDemo implements Runnable {

	@Override
	public void run() {
		Producer<String, String> producer = new KafkaProducer<>(getProperties());
		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<String, String>("nee", "nee:" + i), new MyCallBack());
		}
		producer.close();
	}

	public static Properties getProperties() {
		Properties props = new Properties();
		/** broker集群地址 */
		props.put("bootstrap.servers", "node86:1132,node101:1132");
		/** 判别请求是否为完整的条件（判断是不是成功发送了）, 我们指定了“all”将会阻塞消息，这种设置性能最低，但是是最可靠的. */
		props.put("acks", "all");
		/** 如果请求失败，生产者会自动重试，我们指定是0次，如果启用重试，则会有重复消息的可能性。 */
		props.put("retries", 0);
		/** 生产者缓存每个分区未发送的消息 */
		props.put("batch.size", 16384);
		/** 异步发送时间间隔 */
		props.put("linger.ms", 1);
		/** 发送端的buffer大小，如果超出将阻塞或者抛出异常由block.on.buffer.full决定 */
		props.put("buffer.memory", 33554432);
		/** key序列化类型 */
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		/** value序列化类型 */
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	public static void main(String[] args) {
		Thread thread1 = new Thread(new ProducerDemo());
		Thread thread2 = new Thread(new ProducerDemo());
		Thread thread3 = new Thread(new ProducerDemo());
		thread1.start();
		thread2.start();
		thread3.start();
	}
}
