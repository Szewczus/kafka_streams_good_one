package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
@Slf4j
public class TopologyExample {
    public Topology createTypology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .peek((k,v)->log.info("1k="+k+"1v="+v))
                .groupBy((key, word) -> word)
                .count(Named.as("Counts"));

        wordCounts.toStream()
                .peek((k,v)->log.info("2k="+k+"2v="+v))
                .foreach((word, count) -> System.out.println("word: " + word + " -> " + count));

        String outputTopic = "word-count-output";
        wordCounts.toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        return topology;
    }
}
