package com.cruise.plugin;

import java.util.HashMap;

import com.corecruise.cruise.services.utils.Services;

public class CruiseQpool {
     private static HashMap<String, CruiseQproducer> producerPool = new HashMap<String, CruiseQproducer>();
     private static HashMap<String, CruiseQconsumer> consumerPool = new HashMap<String, CruiseQconsumer>();
     public static CruiseQproducer createProducer(Services service) throws Exception{
    	 CruiseQproducer ret = null;
    	 String connectName = service.Parameter("connectionName");
    	 if(producerPool.containsKey(connectName.trim())) {
    		 ret = producerPool.get(connectName.trim());
    	 }else {
    		 try {
    			 ret = new CruiseQproducer(service);
    			 producerPool.put(connectName.trim(), ret);
    		 }catch(Exception e) {
    			 throw e;
    		 }
    	 }
         return ret;
     }
     public static CruiseQconsumer createConsumer(Services service) throws Exception{
    	 CruiseQconsumer ret = null;
    	 String connectName = service.Parameter("connectionName");
    	 if(consumerPool.containsKey(connectName.trim())) {
    		 ret = consumerPool.get(connectName.trim());
    	 }else {
    		 try {
    			 ret = new CruiseQconsumer(service);
    			 consumerPool.put(connectName.trim(), ret);
    		 }catch(Exception e) {
    			 throw e;
    		 }
    	 }
         return ret;
     }
     
     
}
