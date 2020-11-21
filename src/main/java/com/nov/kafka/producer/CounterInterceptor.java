package com.nov.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 时间戳拦截器
 * 为了示例，这个用了两个拦截器作为演示
 * @author november
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private long successNUm=0;
    private long errorNum=0;

    /**
     * 在消息发送到kafka服务器之前对消息进行处理
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        System.out.println("CounterInterceptor -- onSend");
        return producerRecord;
    }

    /**
     * 在收到kafka服务器响应之后(包含正常响应和异常响应)，对信息进行处理，会先于用户自定义的回调方法
     * 逻辑
     * 统计成功和失败一共有多少次
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("CounterInterceptor -- onAcknowledgement");
        if(e==null){
            //成功
            successNUm++;
        }else{
            //异常
            errorNum++;
        }
    }

    /**
     * 最后调用
     */
    @Override
    public void close() {
        System.out.println("CounterInterceptor -- close");
        System.out.println("成功："+successNUm+" -- 失败："+errorNum);
    }

    /**
     * 初始化
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("CounterInterceptor -- configure");

    }
}
