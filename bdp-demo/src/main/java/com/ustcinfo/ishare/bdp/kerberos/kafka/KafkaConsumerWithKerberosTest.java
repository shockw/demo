package com.ustcinfo.ishare.bdp.kerberos.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerWithKerberosTest {
	public static void main(String[] args) {
		Properties properties = new Properties();
		/**kafka集群地址及端口号，多台机器用逗号分隔**/
		properties.put("bootstrap.servers", "node86:9092,node99:9092,node101:9092");
		properties.put("acks", "all");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("session.timeout.ms", "180000");
		properties.put("max.poll.records", "100");
		properties.put("auto.offset.reset", "earliest");
		properties.put("auto.commit.interval.ms", "500");
		properties.put("max.poll.interval.ms", "5000");
		properties.put("enable.auto.commit", "false");
		properties.put("heartbeat.interval.ms", "3000");
		properties.put("request.timeout.ms", "300000");
		properties.put("max.poll.interval.ms", "300000");
		/**Kerberos服务名,对应于kafka-jaas.conf中的serviceName**/
		properties.put("sasl.kerberos.service.name", "kafka");
		/**服务器端的安全协议,此处是Kerberos认证,所以是SASL_PLAINTEXT**/
		properties.put("security.protocol", "SASL_PLAINTEXT");
		/**消费者组名**/
		properties.put("group.id", "testgroup");
		/**添加Kerberos认证所需的JAAS配置文件到运行时环境**/
		System.setProperty("java.security.auth.login.config","/Users/shock/git/demo/bdp-demo/src/main/resources/kafka-jaas.conf");
		/**添加krb5配置文件到运行时环境**/
		System.setProperty("java.security.krb5.conf", "/Users/shock/git/demo/bdp-demo/src/main/resources/krb5.conf");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		/**订阅的Topic**/
		consumer.subscribe(Arrays.asList("testTopic"));
		while (true) {
             /**消费者订阅topic后,调用poll方法,加入到组,要留在组中,必须持续调用poll方法**/
			 try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
             ConsumerRecords<String, String> records = consumer.poll(100);
             for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic()+" --- "+ record.partition());
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(),record.key(),record.value()+"     \r\n");
            }         
        }

	}
}
