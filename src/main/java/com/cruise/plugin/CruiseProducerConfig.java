package com.cruise.plugin;

import java.util.Properties;

import com.corecruise.cruise.services.interfaces.CruiseConfigurationInterface;

public class CruiseProducerConfig implements CruiseConfigurationInterface{
    private Properties props = null;

	public boolean initConfig() {
		if(null == props) {
			props = new Properties();
			props.put("accessKey", "AKIAIWWOG4VVUD5XUHRA");
			props.put("secretKey", "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");
		}
		return true;
	}

	public Properties getConfig() {
		// TODO Auto-generated method stub
		return props;
	}

}
