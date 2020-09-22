package com.githug.francescom.sks.serializers;

import static com.githug.francescom.sks.serializers.JsonSerdeFactory.CLASSOBJ_PROP;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerdeFactory {
    final public static String CLASSOBJ_PROP = "CLASSOBJ";

    public static <T> Serde<T> getSerde(Class<T> tClass){
        Map<String, Object> serdeProps = new HashMap<>(2);
        final Serializer<T> pageViewSerializer = new JsonPOJOSerializer<>();
        serdeProps.put(CLASSOBJ_PROP, tClass);
        pageViewSerializer.configure(serdeProps, false);

        final Deserializer<T> pageViewDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put(CLASSOBJ_PROP, tClass);
        pageViewDeserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(pageViewSerializer, pageViewDeserializer);
    }
}
class JsonPOJODeserializer<T> implements Deserializer<T> {
    private static final Gson gson = new Gson();
    private Class<T> tClass;

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJODeserializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get(CLASSOBJ_PROP);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        try {
            data = gson.fromJson(new String(bytes),tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}
class JsonPOJOSerializer<T> implements Serializer<T> {
    private static final Gson gson = new Gson();
    private Class<T> tClass;
    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {

    }
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get(CLASSOBJ_PROP);
    }
    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return gson.toJson(data).getBytes();
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}