package com.githug.francescom.sks.controller;

import com.githug.francescom.sks.components.BeanPostProcessorImplementation;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StreamingController {
  private final Logger logger = LoggerFactory.getLogger(StreamingController.class);

  private final Map<String, KafkaStreams> allStreams;


  @Autowired
  public StreamingController(BeanPostProcessorImplementation postProcessorImplementation){
    this.allStreams = postProcessorImplementation.kafkaBeansMap;
  }


  @RequestMapping(value = "/streams/state", method = RequestMethod.GET)
  @ResponseBody
  public List<String> getState(@RequestParam(required = false) String name) {
    List<String> stateLists = allStreams.entrySet().stream()
        .filter(stringKafkaStreamsEntry -> {
          if (name == null || name.isEmpty()) {
            return true;
          }
          return name.equals(stringKafkaStreamsEntry.getKey());
        }).map(s -> s.getKey() +" -> "+s.getValue().state()).collect(Collectors.toList());
    return stateLists;
  }

}
