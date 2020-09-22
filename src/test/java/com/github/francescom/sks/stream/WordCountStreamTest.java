package com.github.francescom.sks.stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.francescom.sks.BaseSpringClassTest;
import com.githug.francescom.sks.config.KafkaBeansConfig;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.streams.KafkaStreams;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

public class WordCountStreamTest extends BaseSpringClassTest {

  @Autowired
  KafkaStreams wordCountStream;

  @Autowired
  TestRestTemplate template;

  //Complete test, checking that the stream work by using the controller
  @Test
  public void testWordCountStream() throws Exception {
    AtomicInteger valueCounter = new AtomicInteger(0);
    List<String> inputStrings = Lists.newArrayList("Lorem ipsum dolor sit amet, consectetur adipiscing elit","sed do eiusmod tempor incididunt ut labore et dolore magna aliqua",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat","Lorem ipsum dolor sit amet, consectetur adipiscing elit");

    inputStrings.forEach(v -> {
      try {
        kafkaTemplate.send(KafkaBeansConfig.WORD_COUNT_INPUT_TOPIC,v).get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    });
    checkAndWaitStreamIsRunning(wordCountStream,15000);
    Thread.sleep(15000);
    MultiValueMap<String,String> countLoremparams = new LinkedMultiValueMap<>();
    countLoremparams.add("word","lorem");

    //Let's ask how many lorem we can find
    MockHttpServletResponse countResponse = mvc.perform(get("/wordcount/count").params(countLoremparams))
        .andReturn().getResponse();

    Assert.assertEquals(200,countResponse.getStatus());
    Long firstLoremCount = Long.valueOf(countResponse.getContentAsString());
    Assert.assertEquals(2,firstLoremCount,0);
    //Lorem count should be 2

    //Now let's insert 1 row through the controller
    MultiValueMap<String,String> addLoremWordParam = new LinkedMultiValueMap<>();
    addLoremWordParam.add("row","Lorem ipsum dolor sit amet");
    mvc.perform(get("/wordcount/insert").params(addLoremWordParam)).andExpect(status().isOk());
    Thread.sleep(5000);
    countResponse = mvc.perform(get("/wordcount/count").params(countLoremparams))
        .andReturn().getResponse();
    Assert.assertEquals(200,countResponse.getStatus());
    Long newCount = Long.valueOf(countResponse.getContentAsString());
    //Expecting lorem count is bigger, equal to 3
    Assert.assertTrue(firstLoremCount < newCount);
    Assert.assertEquals(3,newCount,0);
  }

}
