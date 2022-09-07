package com.rocketmq.mq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 有序消息消费
 */
public class OrderConsumer {
    public static void main(String[] args) throws MQClientException {
        //1、创建DefaultMQPushConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_producer_order_group");
        //2、设置namesrv地址
        consumer.setNamesrvAddr("192.168.132.129:9876");
        //设置消息拉取上限
        consumer.setConsumeMessageBatchMaxSize(2);
        //3、设置subscribe，这里是要读取的主题信息
        consumer.subscribe("Topic_Order_Demo",//指定要消费的主题
                "*"//过滤规则
        );
        //4、创建消息监听MessageListener
        consumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext consumeOrderlyContext) {
                //读取消息
                for (MessageExt msg : msgs){
                    try {
                        //获取主题
                        String topic = msg.getTopic();
                        //获取标签
                        String tags = msg.getTags();
                        //获取信息
                        String result = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("Order--Consumer消费信息----topic："+topic+"，tags："+tags+"，result："+result);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        //消息重试
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        //开启Consumer
        consumer.start();
    }
}
