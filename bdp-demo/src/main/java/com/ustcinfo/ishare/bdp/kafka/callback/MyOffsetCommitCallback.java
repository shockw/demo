/**
 * @Title: OffsetCallBack.java
 * @Package com.ustcinfo.ishare.eip.callback
 * @Description: 
 * Copyright:© 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights reserved.
 * 
 * @author Ni.Liang
 * @date 2017年3月30日 上午11:00:19
 * @version V1.0
 */
package com.ustcinfo.ishare.bdp.kafka.callback;

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

/**
 * @describe 消费完成后，手动提交offset时候的操作，可自定义业务。
 * @author Nee
 * @mail ni.liang@ustcinfo.com
 * @date 2017年3月30日上午11:00:19
 * @copyright © 2017.Anhui USTC Sinovate Cloud Techonlogy Co., Ltd. All rights
 *            reserved.
 *
 */
public class MyOffsetCommitCallback implements OffsetCommitCallback {

	@Override
	public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
		System.out.println("消费完成，消费的offset为：" + offsets);
	}

}
