package com.github.francescom.sks.config;

import com.githug.francescom.sks.config.ApplicationProperties;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.mock.env.MockEnvironment;

/**
 * Checking if the @Validated annotation on ApplicationProperties class
 * works as expected
 */
public class ApplicationPropertiesTest {
  private AnnotationConfigApplicationContext context;


  @Test
  public void testIncompleteAppProperties(){
    MockEnvironment env = new MockEnvironment();
    //env.setProperty("server.port", "8091");
    //env.setProperty("spring.application-name", "Test");
    env.setProperty("spring.kafka.bootstrap-servers","localhost:9092");
    env.setProperty("spring.kafka.streams.properties.application.server","localhost:8092");
    this.context = new AnnotationConfigApplicationContext();
    this.context.setEnvironment(env);
    this.context.register(ApplicationProperties.class);
    try {
      this.context.refresh();
      Assert.fail();
    }
    catch (Exception ex) {
      System.out.println(ex.getMessage());
    }
  }

  @Test
  public void testGoodApp(){
    MockEnvironment env = new MockEnvironment();
    env.setProperty("server.port", "8081");
    env.setProperty("spring.application-name", "Test");
    env.setProperty("spring.kafka.bootstrap-servers","localhost:9092");
    env.setProperty("spring.kafka.streams.properties.application.server","localhost:8081");
    this.context = new AnnotationConfigApplicationContext();
    this.context.setEnvironment(env);
    this.context.register(ApplicationProperties.class);
    this.context.refresh();
  }

}
