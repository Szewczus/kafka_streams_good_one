package com.example.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateTopic {
    @Bean
    public NewTopic libraryEvents(){
        return TopicBuilder.name("word-count-input").partitions(2).replicas(1).build();
    }
}
