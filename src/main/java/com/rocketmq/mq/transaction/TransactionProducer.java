package com.rocketmq.mq.transaction;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.*;

/**
 * 消息生产
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException,
            RemotingException, InterruptedException {
        //1、TransactionMQProducer
        TransactionMQProducer producer = new TransactionMQProducer("demo_producer_transaction_group");
        //2、设置Namesrv地址
        producer.setNamesrvAddr("192.168.132.129:9876");
        //指定消息监听对象，用于执行本地事务和消息回查
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);
        //线程池
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(
                        2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                }
        );
        producer.setExecutorService(executorService);
        //3、开启创建DefaultMQProducer
        producer.start();
        //4、创建Message
        Message message = new Message("Transaction",//主题
                "Tags",//标签，主要用于消息过滤
                "Keys_T",//消息的唯一值
                "hello!-Transaction".getBytes(RemotingHelper.DEFAULT_CHARSET) );
        //5、发送事务消息
        TransactionSendResult result = producer.sendMessageInTransaction(message,"hello-transaction");
        System.out.println(result);
        //6、关闭创建DefaultMQProducer
        producer.shutdown();
    }
}