package com.github.francescom.sks.serializers;

import java.io.Serializable;
import java.util.List;
import org.assertj.core.util.Lists;

public class TestClass implements Serializable{

  public String key;
  public Integer value;
  public Long ts;
  public Long aggregatedValuesCount;
  public String stringValue;
  public Double number;
  public List<String> values;

  public TestClass() {
  }

  public TestClass(String key, Integer value) {
    this.key = key;
    this.value = value;
    this.ts = System.currentTimeMillis();
    this.aggregatedValuesCount = 0L;
  }

  public TestClass(String key, Integer value, long ts) {
    this.key = key;
    this.value = value;
    this.ts = ts;
    this.aggregatedValuesCount = 0L;
  }

  public TestClass(String key, Integer value, Long aggregatedValuesCount,
      String stringValue) {
    this.key = key;
    this.value = value;
    this.ts = System.currentTimeMillis();
    this.aggregatedValuesCount = aggregatedValuesCount;
    this.stringValue = stringValue;
    this.number = Math.random()*100;
    this.values = Lists.list("aaaa","aa","bb","cc","DD","a","cc");
  }

  @Override
  public String toString() {
    return "TestClass{" +
        "key='" + key + '\'' +
        ", value=" + value +
        ", ts=" + ts +
        ", aggregatedValuesCount=" + aggregatedValuesCount +
        '}';
  }
}
