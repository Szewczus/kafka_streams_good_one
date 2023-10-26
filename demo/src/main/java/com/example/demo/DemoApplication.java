package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
@Slf4j
public class DemoApplication {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application1");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        StreamsBuilder builder = new StreamsBuilder();
        //wordCountInput(properties, builder);
        favouriteColour(properties, builder);
        Topology typology = builder.build();
        KafkaStreams streams = new KafkaStreams(typology, properties);
        streams.start();
        System.out.println("streams="+streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


//        TopologyExample topology = new TopologyExample();
//        try {
//            KafkaStreams streams = new KafkaStreams(topology.createTypology(), properties);
//            streams.start();
//            System.out.println(streams.toString());
//            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//        }
//        catch (Exception e){
//            log.info(e.toString());
//        }


    }

    private static void wordCountInput(Properties properties, StreamsBuilder builder) {
        KStream<String, String> kStream = builder.stream("word-count-input");
        lesson16(properties, builder, kStream);
    }

    private static void favouriteColour(Properties properties, StreamsBuilder builder){
        KStream<String, String> kStream = builder.stream("favourite-colour1");
        KStream<String, String> colorsSmallLetter = kStream
                .selectKey((k, v)-> k.toLowerCase())
                .mapValues(v-> v.toLowerCase());

        colorsSmallLetter.to("favourite-colours-process1", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> colorsSmallLetterTable = builder.table("favourite-colours-process1");

        KTable<String, Long> colorsCount= colorsSmallLetterTable
                .groupBy((k, v)-> new KeyValue<>(v, v))
                .count();
        colorsCount.toStream().to("favourite-colour-output1", Produced.with(Serdes.String(), Serdes.Long()));
    }


    private static void lesson16(Properties properties, StreamsBuilder builder, KStream<String, String> kStream) {
        //kStream.to("word-count-output");
        kStream.mapValues(v->v.toLowerCase())
                .flatMapValues(l-> Arrays.asList(l.split(" ")))
                .groupBy((key, word)->word)
                .count()
                .toStream()
                .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    }





}
