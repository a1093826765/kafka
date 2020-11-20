package com.nov.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 生产者
 * @author november
 */
public class CustomProducer {

    private static final int NUM=1000;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //配置kafka参数
        Properties properties = new Properties();
        //kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,1);
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);


        //创建1个生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //调用send()方法
        for(int i=0;i<NUM;i++){
            RecordMetadata recordMetadata = kafkaProducer.send(new ProducerRecord<String, String>("bbb", i + "", "message-" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //数据发送成功
                        System.out.println("success");
                    } else {
                        //数据发送失败
                        e.printStackTrace();
                    }
                }
            }).get();//这里添加get()后，阻塞功能，同步发送，效率会比异步低
            System.out.println("meta："+recordMetadata.offset()+" -- "+recordMetadata.topic());
        }

        //关闭生产者
        kafkaProducer.close();
    }
}
