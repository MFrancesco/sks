package com.githug.francescom.sks.utils;

import com.google.common.collect.Lists;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class MessageStorerConsumer<K,V> extends BaseConsumer<K,V> {
    private final List<V> consumerValuesList;
    private final List<ConsumerRecord<K,V>> consumerRecordList;
    private final Set<K> receivedKeysSet;

    public MessageStorerConsumer(String topic,Map<String,Object> consumerProperties,
                                 Deserializer<K> keyDeserializer,
                                 Deserializer<V> valueDeserializer) {
        super(topic, consumerProperties,keyDeserializer,valueDeserializer);
        this.consumerValuesList = new CopyOnWriteArrayList<>();
        this.consumerRecordList = new LinkedList<>();
        this.receivedKeysSet = new CopyOnWriteArraySet<>();
    }

    public MessageStorerConsumer(String topic,KafkaConsumer<K,V> consumer) {
        super(Lists.newArrayList(topic), consumer);
        this.consumerValuesList = new CopyOnWriteArrayList<>();
        this.consumerRecordList = new LinkedList<>();
        this.receivedKeysSet = new CopyOnWriteArraySet<>();
    }

    @Override
    public boolean consumeRecords(ConsumerRecords<K, V> consumerRecords) {
        consumerRecords.forEach(cr -> {
            consumerValuesList.add(cr.value());
            consumerRecordList.add(cr);
            receivedKeysSet.add(cr.key());
        });
        return true;
    }

    public List<V> getReceivedMessages(){
        return consumerValuesList;
    }

    public Set<K> getReceivedKeysSet(){
        return receivedKeysSet;
    }

    public List<ConsumerRecord<K,V>> getConsumerRecordList(){
        return consumerRecordList;
    }

    public void flushReceivedMessages(){
      this.consumerRecordList.clear();
      this.consumerValuesList.clear();
      this.receivedKeysSet.clear();
    }
}
