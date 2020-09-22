package com.githug.francescom.sks.config;

import com.githug.francescom.sks.components.TopicManager;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration that will be loaded before the KafkaConfig,
 * place here all the initialization you need to do, such as creation of topics
 */
@Configuration
public class KafkaInitializationConfig {
  public static Logger logger = LoggerFactory.getLogger(KafkaBeansConfig.class);

  @Value("${spring.kafka.num-partitions:11}")
  String numPartitions;

  @Value("${spring.kafka.replication-factor:1}")
  String replicationFactor;

  /**
   * List ot the initial topics to create at the startup of the app
   * Cause i'm not an autoCreateTopic guy
   */
  @Value("${spring.kafka.initial-topics:}#{T(java.util.Collections).emptyList()}")
  private List<String> initialTopics;

  @Bean
  @Autowired
  Boolean topicInitalized(TopicManager topicManager) {
    logger.info("Topic to create list is " + Arrays.toString(initialTopics.toArray()));
    if (!initialTopics.isEmpty()){
      topicManager.createTopics(Short.valueOf(numPartitions), Short.valueOf(replicationFactor),
          initialTopics);
    }
    return true;
  }

}
