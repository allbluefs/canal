package com.wwjd.canal.canaltest.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Date:2019/9/16
 */
@RestController
@RequestMapping("/kafka")
public class KafkaSender {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/send")
    public String send(String msg){
        kafkaTemplate.send("example", msg);
        return "success";
    }
}
