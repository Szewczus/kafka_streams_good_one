package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.objenesis.instantiator.annotations.Typology;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, String> kStream = builder.stream("word-count-input");
//        //kStream.to("word-count-output");
//        kStream.mapValues(v->v.toUpperCase())
//                .flatMapValues(l-> Arrays.asList(l.split(" ")))
//                .groupBy((key, word)->word)
//                .count()
//                .toStream()
//                .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
//        Topology typology = builder.build();
//        KafkaStreams streams = new KafkaStreams(typology, properties);
//        streams.start();
//        System.out.println(streams.toString());
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


        TopologyExample topology = new TopologyExample();
        KafkaStreams streams = new KafkaStreams(topology.createTypology(), properties);
        streams.start();
        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }


}
