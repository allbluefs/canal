package com.wwjd.canal.canaltest.kafka;


import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @Date:2019/9/16
 */
@Component
public class KafkaConsumer1 {
    @KafkaListener(topics = "${kafka.test.topic}",groupId = "fangsong1")
    public void listen (ConsumerRecord<?, ?> record) throws Exception {
        System.out.println("kafka11111111消费==============================");
        System.out.printf("topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
//        转换数据格式
        Map map = JSON.parseObject((String) record.value(), Map.class);
    }
}
