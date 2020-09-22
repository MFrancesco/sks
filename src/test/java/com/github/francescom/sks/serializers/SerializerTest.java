package com.github.francescom.sks.serializers;

import com.githug.francescom.sks.serializers.CompressedSerializerFactory;
import com.githug.francescom.sks.serializers.JsonSerdeFactory;
import com.githug.francescom.sks.serializers.KyroSerdeFactory;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Test;

/**
 * Benchmark performed to compare the performances of
 * different serializers
 */
public class SerializerTest {

  @Test
  public void serializationBenchmarks(){
    final int n = 10000;
    List<TestClass> instancesList = IntStream.range(1, n + 1).mapToObj(i ->
        new TestClass("KEY" + i, i % 33, 0l, "Value" + i % 6)
    ).collect(Collectors.toList());
    final Serde<TestClass> jsonSerde = JsonSerdeFactory.getSerde(TestClass.class);
    final Serde<TestClass> compressedByteSerde = CompressedSerializerFactory.getSerde(TestClass.class);
    final Serde<TestClass> kyroSerde = KyroSerdeFactory.getUncompressedSerde(TestClass.class,false);
    final Serde<TestClass> kyroSerdeCompressed = KyroSerdeFactory.getCompressedSerde(TestClass.class,false);
    System.out.println("JSON serde benchmark");
    performBenchmark(instancesList,jsonSerde);
    System.out.println("Compressed java serde benchmark");
    performBenchmark(instancesList,compressedByteSerde);
    System.out.println("Compressed Kyro serde benchmark");
    performBenchmark(instancesList,kyroSerdeCompressed);
    System.out.println("Kyro serde benchmark");
    performBenchmark(instancesList,kyroSerde);
  }

  private void performBenchmark(List<TestClass> instancesList,
      Serde<TestClass> serde) {
    System.out.println("Running benchmarks on " + instancesList.size() + " instances");
    Tick serializationTick = new Tick();
    List<byte[]> bytesList = instancesList.parallelStream()
        .map(obj -> serde.serializer().serialize("", obj)).collect(
            Collectors.toList());
    System.out.println("Serialization done in millis " + serializationTick.getTock());
    Tick deserializationTick = new Tick();
    List<TestClass> objects = bytesList.parallelStream()
        .map(bytes -> serde.deserializer().deserialize("", bytes))
        .collect(Collectors.toList());
    System.out.println("Deserialization done in millis " + deserializationTick.getTock());
    final double totalBytes = bytesList.stream().mapToDouble(b -> b.length).sum();
    System.out.println("Total space in bytes: " + totalBytes);
  }

  class Tick {

    private final long time;

    Tick(){
      this.time = System.currentTimeMillis();
    }

    long getTock(){
      return System.currentTimeMillis() - this.time;
    }
  }
}
