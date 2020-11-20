package com.nov.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者
 * @author november
 */
public class CustomProducer {

    private static final int NUM=1000;

    public static void main(String[] args) {
        //配置kafka参数
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);

        //创建1个生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //调用send()方法
        for(int i=0;i<NUM;i++){
            kafkaProducer.send(new ProducerRecord<String, String>("bbb", i + "", "message-" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        //数据发送成功
                        System.out.println("success");
                    }else{
                        //数据发送失败
                        e.printStackTrace();
                    }
                }
            });
        }

        //关闭生产者
        kafkaProducer.close();
    }
}
