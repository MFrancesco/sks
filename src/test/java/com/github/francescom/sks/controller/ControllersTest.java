package com.github.francescom.sks.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.francescom.sks.BaseSpringClassTest;
import com.githug.francescom.sks.config.KafkaBeansConfig;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

public class ControllersTest extends BaseSpringClassTest {

  @Autowired
  private TestRestTemplate template;

  //just checking the validation
  @Test
  public void testControllerIntputValidation() throws Exception {
    int status = mvc.perform(get("/wordcount/count")).andReturn().getResponse().getStatus();
    Assert.assertEquals(400,status);
    MultiValueMap<String,String> params = new LinkedMultiValueMap<>();
    params.add("word","");
    MockHttpServletResponse res = mvc
        .perform(get("/wordcount/count").params(params)).andReturn().getResponse();
    Assert.assertEquals(400,res.getStatus());
    System.out.println(res.getContentAsString());
  }

  @Test
  public void streamControllerTest() throws Exception {
    mvc.perform(get("/stream/state")).andExpect(status().isNotFound());

    mvc.perform(get("/streams/state")).andExpect(status().isOk());

    MockHttpServletResponse response = mvc.perform(get("/streams/state")).andReturn().getResponse();
    Assert.assertEquals(200,response.getStatus());
    System.out.println(response.getContentAsString());
    MultiValueMap<String,String> params = new LinkedMultiValueMap<>();
    params.add("name", KafkaBeansConfig.WORD_COUNT_STREAMNAME);

    response = mvc.perform(get("/streams/state").params(params)).andReturn().getResponse();

    Assert.assertEquals(200,response.getStatus());
    System.out.println(response.getContentAsString());

     params = new LinkedMultiValueMap<>();
    params.add("name", "pippo");

    response = mvc.perform(get("/streams/state").params(params)).andReturn().getResponse();
    Assert.assertEquals(200,response.getStatus());
    Assert.assertEquals("[]",response.getContentAsString());
  }


}
