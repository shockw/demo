/**
 * @Title: MyCallBack.java
 * @Package com.ustcinfo.ishare.eip
 * @Description: 
 * Copyright:© 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights reserved.
 * 
 * @author Ni.Liang
 * @date 2017年3月29日 下午5:27:27
 * @version V1.0
 */
package com.ustcinfo.ishare.bdp.kafka.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @describe 生产者生产完消息后可进行的一个callback操作。可根据业务需求添加惭怍
 * @author Nee
 * @mail ni.liang@ustcinfo.com
 * @date 2017年3月29日下午5:27:27
 * @copyright © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 *            reserved.
 *
 */
public class MyCallBack implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		System.out.println(Thread.currentThread().getName() + " 发送成功啦" + ",发送到的分区为： " + metadata.partition());
	}

}
