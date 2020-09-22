package com.githug.francescom.sks.utils;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseConsumer<K,V> implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    private final List<String> topics;
    private KafkaConsumer consumer;
    public AtomicBoolean shouldContinue = new AtomicBoolean(true);

    public BaseConsumer(List<String> topics,KafkaConsumer<K,V> consumer){
     this.topics = topics;
     this.consumer = consumer;
    }

    public BaseConsumer(List<String> topics,
                        Map<String,Object> consumerProperties,
                        Deserializer<K> keyDeserializer,
                        Deserializer<V> valueDeserializer){
        this.topics = topics;
        this.consumer = new KafkaConsumer<K,V>(consumerProperties,keyDeserializer,valueDeserializer);
    }

    public BaseConsumer(String topic,
                        Map<String,Object> consumerProperties,
                        Deserializer<K> keyDeserializer,
                        Deserializer<V> valueDeserializer){
        this(Collections.singletonList(topic),consumerProperties,keyDeserializer,valueDeserializer);
    }

    @Override
    public void run(){
        try {
            consumer.subscribe(topics);
            while (shouldContinue.get()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(200 ));
              if (!consumeRecords(records)) {
                logger.warn("problem while consuming record");
              } else {
                consumer.commitSync();
              }
            }
        }catch (WakeupException e){
            logger.info("WakeUpException , requested shutdown of consumer");
        }
        catch (Exception e){
            logger.error(e.getMessage(),e);
        }
        finally {
            consumer.close(Duration.ofMillis(30000));
        }

    }

    /**
     * Consume the records in input returning true if they have been consumed correctly, false otherwise
     * @param consumerRecords
     * @return
     */
    public abstract boolean consumeRecords(ConsumerRecords<K,V> consumerRecords);

    public void close(){
        shouldContinue.set(false);
        consumer.wakeup();
    }

}