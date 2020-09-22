package com.github.francescom.sks;


import com.githug.francescom.sks.Application;
import com.githug.francescom.sks.utils.MessageStorerConsumer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;


@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT, classes = Application.class,properties = "server.port=8081")
@ActiveProfiles("test")
@AutoConfigureMockMvc
public class BaseSpringClassTest {

  static int NUMBER_OF_KAFKA_SERVERS = 3;

  @ClassRule
  public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(NUMBER_OF_KAFKA_SERVERS, true);

  @Autowired
  protected MockMvc mvc;

  static ExecutorService executorService;

  @Autowired
  private TestRestTemplate template;

  @Autowired
  protected KafkaTemplate<String,String> kafkaTemplate;

  @BeforeClass
  public static void beforeClass(){
    executorService = Executors.newFixedThreadPool(10);
      System.setProperty("spring.kafka.bootstrap-servers",
          embeddedKafka.getEmbeddedKafka().getBrokersAsString());
      System.setProperty("server.port","8081");
      //Raising the update frequency to 1 sec so the aggregations are
      //Performed more frequently
  }

  @Autowired
  ApplicationContext applicationContext;


  @Test
  public void contextLoadedCorrectly(){
    Assert.assertNotNull(applicationContext);
  }

  @Autowired
  protected KafkaProperties kafkaProperties;

  protected Properties toProperties(Map<String,Object> mapProperties){
    Properties p = new Properties();
    p.putAll(mapProperties);
    return p;
  }

  protected void startRunnable(Runnable r){
    executorService.execute(r);
  }

  protected <K,V> MessageStorerConsumer<K,V> getAndStartMessageStorerConsumer(String topic,Serde<K> keySerde,
      Serde<V> valueSerde){

    KafkaConsumer<K,V> kafkaConsumer = new KafkaConsumer<K, V>(
        toProperties(kafkaProperties.buildConsumerProperties()),keySerde.deserializer(),valueSerde.deserializer());

    MessageStorerConsumer<K,V> storerConsumer = new MessageStorerConsumer<>(topic,kafkaConsumer);
    startRunnable(storerConsumer);
    return storerConsumer;
  }

  protected void checkAndWaitStreamIsRunning(KafkaStreams kafkaStreams,long maxMillisToWaitBeforeCrash){
    long now = System.currentTimeMillis();
    while (!kafkaStreams.state().isRunning()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("Waiting till stream is running");
      if (System.currentTimeMillis()-now>=maxMillisToWaitBeforeCrash){
        //It took too much time to go up and running
        Assert.fail();
      }
    }
  }

}
