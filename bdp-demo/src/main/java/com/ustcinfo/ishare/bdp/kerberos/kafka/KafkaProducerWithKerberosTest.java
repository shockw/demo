package com.ustcinfo.ishare.bdp.kerberos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaProducerWithKerberosTest {
	public static void main(String[] args) {
		Properties properties = new Properties();
		/** kafka集群地址及端口号，多台机器用逗号分隔 **/
		properties.put("bootstrap.servers",
				"node86:9092,node99:9092,node101:9092");
		properties.put("acks", "all");
		properties.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put("kafka.retries", "3");
		properties.put("batch.size", "16880");
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		/** Kerberos服务名,对应于kafka-jaas.conf中的serviceName **/
		properties.put("sasl.kerberos.service.name", "kafka");
		/** 服务器端的安全协议,此处是Kerberos认证,所以是SASL_PLAINTEXT **/
		properties.put("security.protocol", "SASL_PLAINTEXT");
		/** 添加Kerberos认证所需的JAAS配置文件到运行时环境 **/
		System.setProperty("java.security.auth.login.config",
				"/Users/shock/git/demo/bdp-demo/src/main/resources/kafka-jaas.conf");
		/** 添加krb5配置文件到运行时环境 **/
		System.setProperty("java.security.krb5.conf", "/Users/shock/git/demo/bdp-demo/src/main/resources/krb5.conf");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
				properties);
		/** Test为Topic,message为发送的消息 **/
		ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(
				"testTopic", "message");
		producer.send(record1);
		producer.close();
	}

}
