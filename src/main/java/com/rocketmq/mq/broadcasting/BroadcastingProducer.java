package com.rocketmq.mq.broadcasting;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class BroadcastingProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException,
            RemotingException, InterruptedException {
        //1、创建DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer("demo_producer_broadcasting_group");
        //2、设置Namesrv地址
        producer.setNamesrvAddr("192.168.132.129:9876");
        //3、开启创建DefaultMQProducer
        producer.start();
        //4、创建Message
        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("Topic_Broadcasting_Demo",//主题
                    "Tags",//标签，主要用于消息过滤
                    "Keys_1"+i,//消息的唯一值
                    ("hello!"+i).getBytes(RemotingHelper.DEFAULT_CHARSET) );
            messages.add(message);
        }

        //5、批量发送消息
        SendResult result = producer.send(messages);
        System.out.println(result);
        //6、关闭创建DefaultMQProducer
        producer.shutdown();
    }
}
