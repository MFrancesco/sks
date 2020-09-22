package com.githug.francescom.sks.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Some utils used in a bunch of project
 */
public class KVStoreUtils {

  private static final Logger logger = LoggerFactory.getLogger(KVStoreUtils.class);
  static RestTemplate restTemplate = new RestTemplate();

  public static synchronized RestTemplate getRestTemplate() {
    if (restTemplate == null){
      restTemplate = new RestTemplate();
    }
    return restTemplate;
  }

  /**
   * Return all keyValues not null contained in a keyValueStore
   */
  public static <K, V> List<KeyValue<K, V>> getAllValueInKeyValueStore(KeyValueStore<K, V> store) {
    LinkedList<KeyValue<K, V>> keyValueLinkedList = new LinkedList<>();
    KeyValueIterator<K, V> iterator = store.all();
    while (iterator.hasNext()) {
      KeyValue<K, V> val = iterator.next();
      if (val.key != null && val.value != null) {
        keyValueLinkedList.add(val);
      }
    }
    iterator.close();
    return keyValueLinkedList;
  }

  /**
   * Return all keys not null contained in a keyValueStore
   */
  public static <K, V> List<K> getAllKeysInValueStore(KeyValueStore<K, V> store) {
    LinkedList<K> keyList = new LinkedList<>();
    KeyValueIterator<K, V> iterator = store.all();
    while (iterator.hasNext()) {
      try {
        keyList.add(iterator.next().key);
      }catch (SerializationException e){

      }
    }
    iterator.close();
    return keyList;
  }

  public static <K,V> Optional<V> retrieveValueFromKVStoreFromGetEndpoint(
      String currentHostnamePort,
      Class<V> valueClass,
      K key,
      Serializer<K> keySerializer,
      String kvStoreName,
      KafkaStreams kafkaStream,
      String endpointUrl,
      Map<String,String> requestParam,
      boolean isHttps) throws IllegalAccessException, InstantiationException {
    HostInfo hostInfo = kafkaStream.metadataForKey(kvStoreName, key, keySerializer).hostInfo();
    String host = hostInfo.host()+":"+hostInfo.port();
    V value;
    if (host.equals(currentHostnamePort)){//Bingo, right instance has been contacted
      ReadOnlyKeyValueStore<K, V> store = kafkaStream
          .store(kvStoreName, QueryableStoreTypes.<K, V>keyValueStore());
      value = store.get(key);
    }else {
      logger.info("We must redirect to another " + host);
      UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl((isHttps?"https://":"http://")+host+endpointUrl);
      for (Map.Entry<String,String> entry : requestParam.entrySet()){
        builder = builder.queryParam(entry.getKey(),entry.getValue());
      }
      value = getRestTemplate().getForEntity(builder.toUriString(),valueClass).getBody();
    }
    return Optional.ofNullable(value);
  }

}