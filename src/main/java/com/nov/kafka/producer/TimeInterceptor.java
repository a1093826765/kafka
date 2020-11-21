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
public class TimeInterceptor implements ProducerInterceptor<String ,String> {

    /**
     * onSend():在消息发送到kafka服务器之前对消息进行处理
     * 逻辑
     * 给value添加一个时间戳
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        System.out.println("TimeInterceptor -- onSend");
        return new ProducerRecord<String, String>(producerRecord.topic(),producerRecord.partition(),producerRecord.timestamp(),producerRecord.key(),System.currentTimeMillis()+producerRecord.value(),producerRecord.headers());
    }

    /**
     * 在收到kafka服务器响应之后(包含正常响应和异常响应)，对信息进行处理，会先于用户自定义的回调方法
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("TimeInterceptor -- onAcknowledgement");
    }

    /**
     * 最后调用
     */
    @Override
    public void close() {
        System.out.println("TimeInterceptor -- close");
    }

    /**
     * 初始化
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("TimeInterceptor -- configure");
    }
}
