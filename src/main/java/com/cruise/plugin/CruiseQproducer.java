package com.cruise.plugin;

import java.util.Properties;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.ActionParameter;
/*
The CLIENT_ID_CONFIG (“client.id”) is an id to pass to the server when making requests so the server can track the source of requests 
beyond just IP/port by passing a producer name for things like server-side request logging.

The KEY_SERIALIZER_CLASS_CONFIG (“key.serializer”) is a Kafka Serializer class for Kafka record keys that implements the Kafka 
Serializer interface. Notice that we set this to LongSerializer as the message ids in our example are longs.

The VALUE_SERIALIZER_CLASS_CONFIG (“value.serializer”) is a Kafka Serializer class for Kafka record values that implements the 
Kafka Serializer interface. Notice that we set this to StringSerializer as the message body in our example are strings.
 */
public class CruiseQproducer {
       public Properties producerProperties = new Properties();
       Producer<String, String> producer = null;
       public String topic = null;
       /**
        * Creates the KafkaProducer
        * 
        * 
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","unknown","This is the name of the Kafka connection that will be used for subsequent writes."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("serverList","true","127.0.0.1", "Comma seperated list of servers"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("topic","true","mytopic", "Name of topic to produce to."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("clientId","true","CruiseEngine.0001", "Client identifier."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("pole","true","1000", "The time in milliseconds that the consumer will wait to recieve a record."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("retryCount","true","10", "Number of time to retry if no records are recieved. -1 waits forever."));
        * @param service
        */
       public CruiseQproducer(Services service) throws Exception{
    	   try {

    		   String bootStrapServers = service.Parameter("serverList");
    		   String clientID = service.Parameter("clientId");
    		   topic = service.Parameter("topic");
    		   producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    		   producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
    		   producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    		   producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    		   for (Entry<String, String> entry : service.getParameters().entrySet()) {
    			   String key = entry.getKey();
    			   if(key.startsWith("config_")) {
    				   if(producerProperties.containsKey(key.replace("config_", ""))) {
    					   producerProperties.remove(key.replace("config_", ""));
    				   }
    				   producerProperties.put(key.replace("config_", ""), entry.getValue());
    			   }
    		   }
    		   producer =  new KafkaProducer<>(producerProperties);
    	   }catch(Exception e) {
    		   throw e;
    	   }

       }
       public RecordMetadata sendSyncMessage(Services service) throws InterruptedException, ExecutionException {
    	   RecordMetadata metadata = null;
    	   long time = System.currentTimeMillis();
    	   String msg = service.Parameter("stringMessage");
    	   try {
    		   final ProducerRecord<String, String> record = new ProducerRecord<>(topic, UUID.randomUUID().toString() , msg);
    		   metadata = producer.send(record).get();
    		   long elapsedTime = System.currentTimeMillis() - time;
    		   System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
    	   } finally {
    		   producer.flush();
    		   //producer.close();
    	   }
    	   return metadata;
       }
       public boolean sendAsyncMessage() {
    	   boolean ret = false;
    	   
    	   
    	   return ret;
       }
}
