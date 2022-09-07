package com.rocketmq.mq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 顺序消息生产
 */
public class OrderProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException,
            RemotingException, InterruptedException {
        //1、创建DefaultMQProducer
        DefaultMQProducer producer = new DefaultMQProducer("demo_producer_order_group");
        //2、设置Namesrv地址
        producer.setNamesrvAddr("192.168.132.129:9876");
        //3、开启创建DefaultMQProducer
        producer.start();

        //5、发送消息
        //第一个参数：发送的消息信息
        //第二个参数：选中指定的消息队列对象（会将所有消息队列传入）
        //第三个参数：指定对应的队列下标
        for (int i = 0; i < 5; i++) {
            //4、创建Message
            Message message = new Message("Topic_Order_Demo",//主题
                    "Tags",//标签，主要用于消息过滤
                    "Keys_"+i,//消息的唯一值
                    ("hello!"+i).getBytes(RemotingHelper.DEFAULT_CHARSET) );
            SendResult result = producer.send(
                    message,
                    new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            //获取队列的下标
                            Integer index = (Integer) arg;
                            //获取对应下标的队列
                            return mqs.get(index);
                        }
                    },
                    1
            );
            System.out.println(result);
        }
        //6、关闭创建DefaultMQProducer
        producer.shutdown();
    }
}
