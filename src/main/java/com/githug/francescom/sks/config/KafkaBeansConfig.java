package com.githug.francescom.sks.config;

import com.githug.francescom.sks.components.TopicManager;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Insert here all the KafkaStreams as BEANS
 */
@Configuration
public class KafkaBeansConfig {
  public static Logger logger = LoggerFactory.getLogger(KafkaBeansConfig.class);
  public static final String WORD_COUNT_STREAMNAME ="wordCountStream";
  public static final String WORD_COUNT_INPUT_TOPIC ="INPUT";
  public static final String WORD_COUNT_OUTPUT_TOPIC = "OUTPUT";
  public static final String WORD_COUNT_COUNT_STORE_NAME = "COUNT_STORE";

  @Autowired
  KafkaProperties kafkaProperties;

  @Autowired
  TopicManager topicManager;

  @Bean(WORD_COUNT_STREAMNAME)
  KafkaStreams wordCountStream(KafkaProperties kafkaProperties){
    //Lets create the topic firsts
    topicManager.createTopics((short) 1,(short)1, Lists.newArrayList(WORD_COUNT_INPUT_TOPIC,WORD_COUNT_OUTPUT_TOPIC));
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    final Pattern pattern = Pattern.compile("\\W+");
    KStream<String, String> inputStream = streamsBuilder
        .stream(WORD_COUNT_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
    KTable<String, Long> counts = inputStream
        .flatMapValues(v -> Arrays.asList(pattern.split(v.toLowerCase())))
        .groupBy((key, value) -> value,Grouped.with(Serdes.String(),Serdes.String()))
        // Count the occurrences of each word (message key).
        .count(Materialized.as(WORD_COUNT_COUNT_STORE_NAME));
    counts.toStream().to(WORD_COUNT_OUTPUT_TOPIC, Produced.with(Serdes.String(),Serdes.Long()));
    Topology topology = streamsBuilder.build();
    KafkaStreams ingestAndAggregateStream = new KafkaStreams(topology,getStreamProperties());
    ingestAndAggregateStream.setStateListener(new ShutdownAppOnStreamShutdownListener("WordCount"));
    return ingestAndAggregateStream;
  }

  private Properties getStreamProperties(){
    Properties p = new Properties();
    p.putAll(kafkaProperties.buildStreamsProperties());
    return p;
  }

  private Properties getStreamProperties(String customApplicationId){
    Properties p = new Properties();
    p.putAll(kafkaProperties.buildStreamsProperties());
    p.put(StreamsConfig.APPLICATION_ID_CONFIG,customApplicationId);
    return p;
  }

  /**
   * Since Streams are crucial we are killing the application
   * when a Stream dies. This way is easier for us to
   */
  class ShutdownAppOnStreamShutdownListener implements StateListener {

    private final String streamId;

    public ShutdownAppOnStreamShutdownListener(String kafkaStreamIdentifier){
      this.streamId = kafkaStreamIdentifier;
    }
    @Override
    public void onChange(State newState, State oldState) {
      if (newState == State.ERROR){
        String msg = String.format("Stream %s went in NOT_RUNNING, will shutdown the application", streamId);
        logger.error(msg);
        System.err.println(msg);
        System.exit(1);
      }

    }
  }
}
