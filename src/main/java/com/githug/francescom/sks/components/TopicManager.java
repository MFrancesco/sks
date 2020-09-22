package com.githug.francescom.sks.components;

import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

@Component
public class TopicManager {

    private static Logger logger = LoggerFactory.getLogger(TopicManager.class.getName());
    private final AdminClient adminClient;

    @Autowired
    public TopicManager(KafkaProperties kafkaProperties){
        this.adminClient = AdminClient.create(kafkaProperties.buildAdminProperties());
    }

    public void createTopic(short numPartitions, short replicationFactor, String topicName){
        createTopics(numPartitions,replicationFactor,Lists.newArrayList(topicName));
    }

    public synchronized void createTopics(short numPartitions, short repliactionFactor, List<String> topicNames){
        topicNames.forEach(topicName -> {
            if(!getExistingTopicNames().contains(topicName)) {
                try {
                    CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(
                        new NewTopic(topicName,numPartitions, repliactionFactor)));
                    createTopicsResult.all().get();
                    logger.info("Created topic " + topicName);
                } catch (TopicExistsException e){
                    logger.warn("java.lang.RuntimeException: java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TopicExistsException: Topic already exists");
                } catch (InterruptedException | ExecutionException e) {
                    if(e.getCause() instanceof TopicExistsException){
                        logger.warn("Topic " + topicName + " already exists");
                    } else {
                        logger.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    public boolean topicExist(String topicName) throws ExecutionException, InterruptedException {
        return adminClient.listTopics().names().get().contains(topicName);
    }

    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        if (getExistingTopicNames().contains(topicName)){
            adminClient.deleteTopics(Lists.newArrayList(topicName)).all().get();
        }
    }

    public Set<String> getExistingTopicNames(){
        try {
            Collection<TopicListing> listTopicsResult = adminClient.listTopics().listings().get();
            return listTopicsResult.stream().map(tpr -> tpr.name()).collect(Collectors.toSet());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return new HashSet<>();
    }

}