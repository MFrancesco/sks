package com.github.francescom.sks.utils;

import com.github.francescom.sks.BaseSpringClassTest;
import com.github.francescom.sks.serializers.TestClass;
import com.githug.francescom.sks.serializers.KyroSerdeFactory;
import com.githug.francescom.sks.utils.MessageStorerConsumer;
import com.githug.francescom.sks.components.TopicManager;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Testing the {@link #getAndStartMessageStorerConsumer(String, Serde, Serde)} method/consumer
 */
public class ProducerConsumerTest extends BaseSpringClassTest {
  @Autowired
  TopicManager topicManager;

  @Autowired
  KafkaTemplate<String,String> kafkaTemplate;


  @Test
  public void producerConsumeTest() throws InterruptedException {
    //KafkaStreams kafkaStreams = new KafkaStreams()
    String testTopic = UUID.randomUUID().toString();
    topicManager.createTopic((short) 1,(short)1,testTopic);
    final Serde<TestClass> testClassSerde = KyroSerdeFactory.getUncompressedSerde(TestClass.class);
    final Serde<String> stringSerde = Serdes.String();

    List<KeyValue<String,TestClass>> list = IntStream.range(1,101)
        .mapToObj(i -> new KeyValue<>(""+i,new TestClass("key"+i,i)))
        .collect(Collectors.toList());

    MessageStorerConsumer<String,TestClass> storerConsumer =
        getAndStartMessageStorerConsumer(testTopic,stringSerde,testClassSerde);

    DefaultKafkaProducerFactory<String,TestClass> kafkaProducerFactory = new DefaultKafkaProducerFactory<String,TestClass>(kafkaProperties.buildConsumerProperties(),
        stringSerde.serializer(),testClassSerde.serializer());
    KafkaTemplate<String,TestClass> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
    list.forEach(l -> {
      try {
        kafkaTemplate.send(testTopic,l.key,l.value).get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    });
    kafkaTemplate.flush();
    Thread.sleep(10000);
    //At least 50 messages has been received
    Assert.assertTrue(storerConsumer.getConsumerRecordList().size()> 50);
    storerConsumer.flushReceivedMessages();
  }
}
