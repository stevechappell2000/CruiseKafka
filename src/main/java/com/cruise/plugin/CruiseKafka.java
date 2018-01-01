package com.cruise.plugin;

import org.apache.kafka.clients.producer.RecordMetadata;
import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.GenericSessionResp;
import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.Action;
import com.cruise.plugins.ActionParameter;
import com.cruise.plugins.PlugInMetaData;


public class CruiseKafka implements PluginInterface
{
	PlugInMetaData pmd = null;
	String QUEUE_NAME = null;
	CruiseProducerConfig config = null;
	public CruiseKafka() {
		config = new CruiseProducerConfig();
		config.initConfig();
		int x = 0;
		
    	pmd = new PlugInMetaData("CruiseKafka","0.0.1","SJC","Database access plugin");
    	
    	pmd.getActions().add(new Action("plugInInfo", "getPlugin Information"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("None","false","unknown","Unused Parameter"));
    	
    	++x;
    	pmd.getActions().add(new Action("Echo", "Test API Call"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("Sample","false","unknown","Unused Parameter"));
		
		++x;
    	pmd.getActions().add(new Action("qCreateProducer", "Create a connection for a Kafka Producer. To include other parameters supported by Kafka client, prefix them with 'config_'."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","unknown","This is the name of the Kafka connection that will be used for subsequent writes."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("serverList","true","127.0.0.1", "Comma seperated list of servers"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("topic","true","mytopic", "Name of topic to produce to."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("clientId","true","CruiseEngine.0001", "Client identifier."));

		++x;
    	pmd.getActions().add(new Action("qCreateConsumer", "Create a connection for a Kafka Producer.  To include other parameters supported by Kafka client, prefix them with 'config_'."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","unknown","This is the name of the Kafka connection that will be used for subsequent writes."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("serverList","true","127.0.0.1", "Comma seperated list of servers"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("topic","true","mytopic", "Name of topic to produce to."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("clientId","true","CruiseEngine.0001", "Client identifier."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("pole","true","1000", "The time in milliseconds that the consumer will wait to recieve a record."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("retryCount","true","10", "Number of time to retry if no records are recieved. -1 waits forever."));
    	
    	++x;
    	pmd.getActions().add(new Action("qSendSyncStringMsg", "Produce a message to the named queue 'connectionName"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","unknown", "Name of connection that was established with the qConnect action"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("stringMessage","true","Sample Message","String value to be posted by Kafka producer."));
    	
    	++x;
    	pmd.getActions().add(new Action("qSendAsynStringMsg", "Produce a message to the named queue 'connectionName"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","unknown", "Name of connection that was established with the qConnect action"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("stringMessage","true","Sample Message","String value to be posted by Kafka producer."));
    	
    	++x;
    	pmd.getActions().add(new Action("qCloseConsumer", "Closes the named consumer"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","unknown", "Name of connection that was established with the qConnect action"));
	}

	public PlugInMetaData getPlugInMetaData() {
		// TODO Auto-generated method stub
		return pmd;
	}

	public void setPluginVendor(PlugInMetaData PMD) {
		pmd = PMD;
		
	}

	public boolean executePlugin(SessionObject so, Services service)  {
		boolean ret = false;
		String action = service.Action().trim().toLowerCase();
		GenericSessionResp gro = new GenericSessionResp();
		QUEUE_NAME = service.Parameter("QName");
		switch (action) {
		case "plugininfo":
			so.appendToResponse(pmd);
			ret = true;
			break;
		case "echo":
			gro.addParmeter("PluginEnabled", "true");
			so.appendToResponse("CruiseTest",gro);
			ret = true;
			break;
		case "qcreateproducer":
			try {
                String connectName = service.Parameter("connectionName");
                if(connectName.length()>0) {
                	try {
                		CruiseQproducer conn = CruiseQpool.createProducer(service);
                		//gro.addObjectParmeter("Producer", "Created");
                		//gro.addObjectParmeter("ProducerTopic", conn.topic);
                		gro.addObjectParmeter("producer", conn.producerProperties);
                		so.appendToResponse(service.Service()+"."+service.Action(),gro);
                		ret = true;
                	}catch(Exception e) {
                		Clog.Error(so, "plugin", "20010", "CruiseWrite for Kafka - qConnect error:"+e.getMessage());
                	}
                }else {
                	Clog.Error(so, "plugin", "20000", "qCreateProducer for Kafka - qConnect requires the connection name");
                }

			}catch(Exception e) {
				e.printStackTrace();
			}
			break;
		case "qcreateconsumer":
			try {
                String connectName = service.Parameter("connectionName");
                if(connectName.length()>0) {
                	try {
                		CruiseQconsumer conn = CruiseQpool.createConsumer(service);
                		conn.runConsumer();
                		gro.addObjectParmeter("consumer", conn.consumerProperties);
                		ret = true;
                	}catch(Exception e) {
                		Clog.Error(so, "plugin", "20010", "CruiseWrite for Kafka - qConnect error:"+e.getMessage());
                	}
                }else {
                	Clog.Error(so, "plugin", "20000", "qCreateConsumer for Kafka - qConnect requires the connection name");
                }

			}catch(Exception e) {
				e.printStackTrace();
			}
			break;
		case "qcloseconsumer":
			try {
                String connectName = service.Parameter("connectionName");
                if(connectName.length()>0) {
                	try {
                		CruiseQconsumer conn = CruiseQpool.createConsumer(service);
                		conn.closeConsumer();
                		ret = true;
                	}catch(Exception e) {
                		Clog.Error(so, "plugin", "20010", "CruiseWrite for Kafka - qConnect error:"+e.getMessage());
                	}
                }else {
                	Clog.Error(so, "plugin", "20000", "qCreateConsumer for Kafka - qConnect requires the connection name");
                }

			}catch(Exception e) {
				e.printStackTrace();
			}
			break;
		case "qsendsyncstringmsg":
			try {
				CruiseQproducer conn = CruiseQpool.createProducer(service);
				if(null != conn) {
					RecordMetadata rmd = conn.sendSyncMessage(service);
			        if(null != rmd) {
						so.appendToResponse(service.Service()+"."+service.Action(), rmd.toString());
						ret = true;
					}else {
						so.appendToResponse(service.Service()+"."+service.Action(), "Failed");
					}
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
			break;
		case "qSendAyncStringMsg":
			try {
				CruiseQproducer conn = CruiseQpool.createProducer(service);
				if(null != conn) {
					RecordMetadata rmd = conn.sendSyncMessage(service);
			        if(null != rmd) {
						so.appendToResponse(service.Service()+"."+service.Action(), rmd);
					}else {
						so.appendToResponse(service.Service()+"."+service.Action(), "Failed");
					}
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
			break;
		default:
			Clog.Error(so, "service", "100.05", "Invalid Action supplied:"+action);
		}


		return ret;
	}

}
