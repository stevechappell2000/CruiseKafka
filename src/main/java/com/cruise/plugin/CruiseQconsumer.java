package com.cruise.plugin;

import java.util.Collections;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.corecruise.cruise.services.utils.Services;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

public class CruiseQconsumer {
	public String topic = null;
	Consumer<String, String> consumer = null;
    int giveUp = 100;   
    boolean running = false;
	int poll = 1000;
	public Properties consumerProperties = new Properties();
	
	public CruiseQconsumer(Services service) throws Exception{
		try {

			String bootStrapServers = service.Parameter("serverList");
			String clientID = service.Parameter("clientId");
			String sGiveUp = service.Parameter("retryCount");
			if(null != sGiveUp && sGiveUp.length()> 0) {
				try {
					giveUp = new Integer(sGiveUp).intValue();
				}catch(Exception e) {
					giveUp=1000;
				}
			}
			String sPole = service.Parameter("pole");
			if(null != sPole && sPole.length()> 0) {
				try {
					poll = new Integer(sPole).intValue();
				}catch(Exception e) {
					poll=1000;
				}
			}
			topic = service.Parameter("topic");
			consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
			consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
			consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,	StringDeserializer.class.getName());
			consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			for (Entry<String, String> entry : service.getParameters().entrySet()) {
				String key = entry.getKey();
				if(key.startsWith("config_")) {
					if(consumerProperties.containsKey(key.replace("config_", ""))) {
						consumerProperties.remove(key.replace("config_", ""));
					}
					consumerProperties.put(key.replace("config_", ""), entry.getValue());
				}
			}
			// Create the consumer using props.
			consumer =	new KafkaConsumer<>(consumerProperties);
			// Subscribe to the topic.
			consumer.subscribe(Collections.singletonList(topic));

		}catch(Exception e) {
			throw e;
		}
	}
	public void closeConsumer() {
		try {
			consumer.close();
			running = false;
		}catch(Exception e) {
			running = false;
		}
	}
	public void runConsumer() throws InterruptedException {
		int noRecordsCount = 0;
		if(running) {
			return;
		}
		/*
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("poll","true","1000", "The time in milliseconds that the consumer will wait to recieve a record."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("retryCount","true","10", "Number of time to retry if no records are recieved. -1 waits forever."));
		 */
		running = true;
		while (true) {
			try {
				final ConsumerRecords<String, String> consumerRecords = consumer.poll(poll);
				if (consumerRecords.count()==0) {
					noRecordsCount++;
					if (noRecordsCount > giveUp) 
						break;
					else 
						continue;
				}
				consumerRecords.forEach(record -> {
					System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
							record.key(), record.value(),
							record.partition(), record.offset());
				});
				consumer.commitAsync();
			}catch(Exception e) {
				e.printStackTrace();
				try {
					consumer.close();
				}catch(Exception ee) {
					ee.printStackTrace();
				}
				break;
			}
		}
		running = false;
	}
}

