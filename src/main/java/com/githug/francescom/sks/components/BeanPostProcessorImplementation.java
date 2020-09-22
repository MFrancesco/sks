package com.githug.francescom.sks.components;

import com.githug.francescom.sks.config.ApplicationProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

/**
 * A simple BeanPostProcessorImplementation
 * Now is only used to add the DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG with value
 * LogAndContinueExceptionHandler on Bean of class KafkaProperties and to start
 * bean of type KafkaStream if the stream is not created
 *
 */
@Component
public class BeanPostProcessorImplementation implements BeanPostProcessor {
  Logger logger = LoggerFactory.getLogger(BeanPostProcessorImplementation.class);

  private final ApplicationProperties appProp;
  public final  Map<String, KafkaStreams> kafkaBeansMap = new HashMap<>();

  @Autowired
  public BeanPostProcessorImplementation(ApplicationProperties appProp){
    this.appProp = appProp;
  }

  public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
    if (bean instanceof KafkaProperties){
      KafkaProperties kafkaProperties = ((KafkaProperties) bean);
      if (kafkaProperties.getStreams().getApplicationId() == null){
        kafkaProperties.getStreams().setApplicationId(appProp.getApplicationName());
      }
      kafkaProperties.getProperties().put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogAndContinueExceptionHandler.class.getName());
      return kafkaProperties;
    }
    return bean;
  }

  public Object postProcessAfterInitialization(Object bean, String beanName)
      throws BeansException {
    if (bean instanceof KafkaStreams){
      KafkaStreams streamBean = (KafkaStreams) bean;
      kafkaBeansMap.put(beanName, streamBean);
      State state = streamBean.state();
      if (state != State.RUNNING && state != State.REBALANCING){
        logger.info("Starting stream {} ",beanName);
        streamBean.cleanUp();
        streamBean.start();
      }
    }


    return bean;
  }


}
