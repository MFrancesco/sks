package com.githug.francescom.sks.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyroSerdeFactory {
  static final String CLASSOBJ_PROP = "CLASSOBJ_PROP";
  public static Logger logger = LoggerFactory.getLogger(KyroSerdeFactory.class);

  public static <T extends Serializable> Serde<T> getCompressedSerde(Class<T> tClass){
    Map<String, Object> serdeProps = new HashMap<>(2);
    final Serializer<T> serializer = new KyroCompressedSerializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    serializer.configure(serdeProps, false);

    final Deserializer<T> deserializer = new KyroCompressedDeserializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    deserializer.configure(serdeProps, false);
    return Serdes.serdeFrom(serializer, deserializer);
  }

  /**
   * return a serialized based on kyro with enabled compression compression
   * @param tClass
   * @param isKey whether is for key or value
   * @param <T>
   * @return
   */
  public static <T extends Serializable> Serde<T> getCompressedSerde(
      Class<T> tClass,
      Boolean isKey){
    Map<String, Object> serdeProps = new HashMap<>(2);
    final Serializer<T> serializer = new KyroCompressedSerializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    serializer.configure(serdeProps, isKey);

    final Deserializer<T> deserializer = new KyroCompressedDeserializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    deserializer.configure(serdeProps, isKey);
    return Serdes.serdeFrom(serializer, deserializer);
  }


  public static <T extends Serializable> Serde<T> getUncompressedSerde(
      Class<T> tClass){
    Map<String, Object> serdeProps = new HashMap<>(2);
    final Serializer<T> serializer = new KyroUncompressedSerializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    serializer.configure(serdeProps, false);

    final Deserializer<T> deserializer = new KyroUncompressedDeserializer<T>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    deserializer.configure(serdeProps, false);
    return Serdes.serdeFrom(serializer, deserializer);
  }

  /**
   * return a serialized based on kyro without compression
   * @param tClass
   * @param isKey whether is for key or value
   * @param <T>
   * @return
   */
  public static <T extends Serializable> Serde<T> getUncompressedSerde(
      Class<T> tClass,
      Boolean isKey){
    Map<String, Object> serdeProps = new HashMap<>(2);
    final Serializer<T> serializer = new KyroUncompressedSerializer<>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    serializer.configure(serdeProps, isKey);

    final Deserializer<T> deserializer = new KyroUncompressedDeserializer<T>();
    serdeProps.put(CLASSOBJ_PROP, tClass);
    deserializer.configure(serdeProps, isKey);
    return Serdes.serdeFrom(serializer, deserializer);
  }

}

class KyroCompressedSerializer<T> implements Serializer<T> {
  //Sets the compression or not
  Class<T> tClass;
  private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      return new Kryo();
    }
  };

  @Override
  public void configure(Map<String, ?> map, boolean isKey) {
    this.tClass = (Class<T>) map.get(KyroSerdeFactory.CLASSOBJ_PROP);
    kryoThreadLocal.get().register(tClass);
  }

  @Override
  public byte[] serialize(String topic, T object) {
    if (object==null)
      return null;
    try {
      Kryo kryo = kryoThreadLocal.get();
      ByteArrayOutputStream byteArrayOutputStream =
          new ByteArrayOutputStream();
      DeflaterOutputStream deflaterOutputStream =
          new DeflaterOutputStream(byteArrayOutputStream);
      Output output = new Output(deflaterOutputStream);
      kryo.writeObject(output, object);
      output.close();
      return byteArrayOutputStream.toByteArray();
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {

  }
}
class KyroCompressedDeserializer<T> implements Deserializer<T> {
  Class<T> tClass;
  private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      return new Kryo();
    }
  };

  @Override
  public void configure(Map<String, ?> map, boolean isKey) {
    this.tClass = (Class<T>) map.get(KyroSerdeFactory.CLASSOBJ_PROP);
    kryoThreadLocal.get().register(tClass);
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null)
      return null;
    try {
      Kryo kyro = kryoThreadLocal.get();
      InputStream in = new ByteArrayInputStream(data);
      in = new InflaterInputStream(in);
      Input input = new Input(in);
      return kyro.readObject(input, tClass);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {

  }
}
class KyroUncompressedSerializer<T> implements Serializer<T> {
  //Sets the compression or not
  Class<T> tClass;
  private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      return new Kryo();
    }
  };

  @Override
  public void configure(Map<String, ?> map, boolean isKey) {
    this.tClass = (Class<T>) map.get(KyroSerdeFactory.CLASSOBJ_PROP);
    kryoThreadLocal.get().register(tClass);
  }

  @Override
  public byte[] serialize(String topic, T object) {
    if (object==null)
      return null;
    Kryo kryo = kryoThreadLocal.get();
    try{
      ByteArrayOutputStream byteArrayOutputStream =
          new ByteArrayOutputStream();
      Output output = new Output(byteArrayOutputStream);
      kryo.writeObject(output,object);
      output.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {

  }
}
class KyroUncompressedDeserializer<T> implements Deserializer<T> {
  Class<T> tClass;
  private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      return new Kryo();
    }
  };

  @Override
  public void configure(Map<String, ?> map, boolean isKey) {
    this.tClass = (Class<T>) map.get(KyroSerdeFactory.CLASSOBJ_PROP);
    kryoThreadLocal.get().register(tClass);
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null)
      return null;
    try{
    Kryo kyro = kryoThreadLocal.get();
    InputStream in = new ByteArrayInputStream(data);
    Input input = new Input(in);
    return kyro.readObject(input,tClass);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {

  }
}