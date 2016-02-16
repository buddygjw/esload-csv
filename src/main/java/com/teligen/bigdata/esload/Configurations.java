package com.teligen.bigdata.esload;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Created by root on 2015/6/24.
 */
public class Configurations {
    private static volatile Configurations instance = null;
    private Configuration config = null;

    private Configurations() {
        String installHome = System.getProperty("install.home", "");
//        String configFilePath = installHome + "/config/load.properties";
        String configFilePath = "C:\\your-work-space\\zhaoch\\project\\esload-csv\\src\\main\\resources\\load.properties";
        try {
            config = new PropertiesConfiguration(configFilePath);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public static Configuration configure() {
        if (instance == null) {
            instance = new Configurations();
        }
        return instance.getConfiguration();
    }

    private Configuration getConfiguration() {
        return config;
    }

    public static void main(String[] args) {
        Configuration configurations = Configurations.configure();
        String clusterName = configurations.getString("es.clustername");

        String[] address = configurations.getStringArray("es.address");

        System.out.printf("clusterName=\\s,address=\\s", clusterName, address);
    }
}
