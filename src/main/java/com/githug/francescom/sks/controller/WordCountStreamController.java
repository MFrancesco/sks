package com.githug.francescom.sks.controller;

import com.githug.francescom.sks.config.KafkaBeansConfig;
import com.githug.francescom.sks.utils.KVStoreUtils;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import javax.validation.constraints.NotBlank;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.hibernate.validator.constraints.Length;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/wordcount")
@Validated
public class WordCountStreamController {
  private final Logger logger = LoggerFactory.getLogger(WordCountStreamController.class);

  private final KafkaStreams wordCountStream;
  private final String localApplicationServer;
  private final KafkaTemplate<String, String> kafkaTemplate;


  @Autowired
  public WordCountStreamController(KafkaProperties properties,KafkaStreams wordCountStream,
      KafkaTemplate<String,String> kafkaTemplate){
    this.wordCountStream = wordCountStream;
    this.localApplicationServer = properties.getStreams().getProperties()
        .get(StreamsConfig.APPLICATION_SERVER_CONFIG);
    this.kafkaTemplate = kafkaTemplate;
  }

  @RequestMapping(value = "/insert", method = RequestMethod.GET)
  public void insertWords(@RequestParam @NotBlank @Length(min = 2) String row){
      kafkaTemplate.send(KafkaBeansConfig.WORD_COUNT_INPUT_TOPIC,row);
  }

  @RequestMapping(value = "/count", method = RequestMethod.GET)
  @ResponseBody
  public Optional<Long> getWordCount(@RequestParam @NotBlank @Length(min = 2) String word){
    try {
      return KVStoreUtils.retrieveValueFromKVStoreFromGetEndpoint(localApplicationServer,
          Long.class,word,Serdes.String().serializer(),
          KafkaBeansConfig.WORD_COUNT_COUNT_STORE_NAME,wordCountStream,"/count",
          ImmutableMap.of("word",word.toLowerCase()),false);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
    return Optional.empty();
  }


}
