package com.githug.francescom.sks.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class CompressedSerializerFactory {
  final public static String CLASSOBJ_PROP = "CLASSOBJ";

  public static <T extends Serializable> Serde<T> getSerde(Class<T> tClass){
    Map<String, Object> serdeProps = new HashMap<>(2);
    final Serializer<T> serializer = new CompressedSerializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    serializer.configure(serdeProps, false);

    final Deserializer<T> deserializer = new CompressedDeserializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    deserializer.configure(serdeProps, false);
    return Serdes.serdeFrom(serializer, deserializer);
  }

  /**
   *
   * @param tClass
   * @param isKey whether is for key or value
   * @param <T>
   * @return
   */
  public static <T extends Serializable> Serde<T> getSerde(Class<T> tClass, Boolean isKey){
    Map<String, Object> serdeProps = new HashMap<>(2);
    final Serializer<T> serializer = new CompressedSerializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    serializer.configure(serdeProps, isKey);

    final Deserializer<T> deserializer = new CompressedDeserializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    deserializer.configure(serdeProps, isKey);
    return Serdes.serdeFrom(serializer, deserializer);
  }
}

class CompressedSerializer<T> implements Serializer<T> {
  private Class<T> tClass;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> map, boolean b) {
    Class<T> tClass = (Class<T>) map.get(CompressedSerializerFactory.CLASSOBJ_PROP);
  }

  @Override
  public byte[] serialize(String s, T t) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
      ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut);
      objectOut.writeObject(t);
      objectOut.close();
      return baos.toByteArray();
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {

  }
}

class CompressedDeserializer<T> implements Deserializer<T> {
  private Class<T> tClass;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> map, boolean b) {
    Class<T> tClass = (Class<T>) map.get(CompressedSerializerFactory.CLASSOBJ_PROP);
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    try {
      if (bytes == null)
        return null;
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      GZIPInputStream gzipIn = new GZIPInputStream(bais);
      ObjectInputStream objectIn = new ObjectInputStream(gzipIn);
      T obj = (T) objectIn.readObject();
      return obj;
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {

  }
}