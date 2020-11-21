package com.nov.kafka.producer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * 消费者
 * @author november
 */
public class CustomConsumer {
    public static void main(String[] args) {
        //配置kafka参数
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        //消费者组，只要group.id相同，就属于同一个消费组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"1205");
        //是否启用自动提交offset(默认true,如果需要手动提交就要给为false)
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        //提交offset时的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

        //创建1个消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //消费者订阅主题
        kafkaConsumer.subscribe(Arrays.asList("bbb"), new ConsumerRebalanceListener() {
            //重新分配，分区之前调用，Rebalance之前调用（用于提交当前负责的分区offset）
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("=================回收的分区==============");
                for(TopicPartition partition:collection){
                    System.out.println("partition："+partition);
                }
            }

            //重新分配，分区之后调用，Rebalance之后调用（用于定位新分配的分区offset ）
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("=================重新得到的分区==============");
                for(TopicPartition partition:collection){
                    System.out.println("partition："+partition);
                    Long offset=getPartitionOffset(partition);
                    kafkaConsumer.seek(partition,offset+1);
                }

            }


        });

        //调用poll
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for(ConsumerRecord<String,String> record:records){
                System.out.println("record="+record.topic()+" -- "+record.offset()+" -- "+record.value());
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                commitOffset(topicPartition,record.offset()+1);
            }
        /*
            //两种提交方式（手动提交）
            //异步，没有提交失败重试机制，效率高
            kafkaConsumer.commitAsync();
            //同步，有提交失败重试机制，效率低
            kafkaConsumer.commitSync();
        */
        }
    }

    private static void commitOffset(TopicPartition topicPartition, long l) {
        //自定义提交
    }


    private static Long getPartitionOffset(TopicPartition partition) {
        //定位新分配的分区offset
        return null;
    }
}
