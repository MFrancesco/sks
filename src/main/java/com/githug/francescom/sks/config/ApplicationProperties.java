package com.githug.francescom.sks.config;

import com.githug.francescom.sks.components.BeanPostProcessorImplementation;
import javax.annotation.PostConstruct;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@Validated
@ConfigurationProperties
public class ApplicationProperties {

  Logger logger = LoggerFactory.getLogger(BeanPostProcessorImplementation.class);
  @NotNull
  @Value("${server.port}")
  Integer serverPort;

  @NotEmpty
  @NotNull
  @Value("${spring.application-name}")
  String applicationName;

  @NotNull
  @NotEmpty
  @Value("${spring.kafka.bootstrap-servers}")
  String kafkaBootStrapServer;

  @NotNull
  @NotEmpty
  @Value("${spring.kafka.streams.properties.application.server}")
  String streamApplicationServer;

  public Integer getServerPort() {
    return serverPort;
  }

  public void setServerPort(Integer serverPort) {
    this.serverPort = serverPort;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public String getKafkaBootStrapServer() {
    return kafkaBootStrapServer;
  }

  public void setKafkaBootStrapServer(String kafkaBootStrapServer) {
    this.kafkaBootStrapServer = kafkaBootStrapServer;
  }
  public String getStreamApplicationServer() {
    return streamApplicationServer;
  }

  public void setStreamApplicationServer(String streamApplicationServer) {
    this.streamApplicationServer = streamApplicationServer;
  }

  @PostConstruct
  private void beanInitialization() {
    if (!streamApplicationServer.contains(":"+serverPort)){
      logger.warn("Wrong configuration, Looks like spring.kafka.streams.properties.application.server "
          + "does not contains the server.port parameter");
    }
  }

}
