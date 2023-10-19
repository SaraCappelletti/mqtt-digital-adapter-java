package it.wldt.adapter.mqtt.digital;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import it.wldt.adapter.mqtt.digital.exception.MqttDigitalAdapterConfigurationException;
import it.wldt.adapter.mqtt.digital.topic.incoming.ActionIncomingTopic;
import it.wldt.adapter.mqtt.digital.topic.outgoing.EventNotificationOutgoingTopic;
import it.wldt.adapter.mqtt.digital.topic.outgoing.PropertyOutgoingTopic;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class MqttDigitalAdapterConfiguration {
    private final String brokerAddress;
    private final Integer brokerPort;
    private String username;
    private String password;
    private final String clientId;
    private boolean cleanSessionFlag = true;
    private Integer connectionTimeout = 10;
    private MqttClientPersistence persistence = new MemoryPersistence();
    private boolean automaticReconnectFlag = true;

    private final Map<String, PropertyOutgoingTopic<?>> propertyUpdateTopics = new HashMap<>();
    private final Map<String, EventNotificationOutgoingTopic<?>> eventNotificationTopics = new HashMap<>();
    private final Map<String, ActionIncomingTopic<?>> actionIncomingTopics = new HashMap<>();


    protected MqttDigitalAdapterConfiguration(String brokerAddress, Integer brokerPort, String clientId) {
        this.brokerAddress = brokerAddress;
        this.brokerPort = brokerPort;
        this.clientId = clientId;
    }

    protected MqttDigitalAdapterConfiguration(String brokerAddress, Integer brokerPort){
        this(brokerAddress, brokerPort, "wldt.mqtt.digital.adapter.client."+new Random(System.currentTimeMillis()).nextInt());
    }

    public static MqttDigitalAdapterConfigurationBuilder builder(String brokerAddress, Integer brokerPort, String clientId) throws MqttDigitalAdapterConfigurationException {
        return new MqttDigitalAdapterConfigurationBuilder(brokerAddress, brokerPort, clientId);
    }

    public static MqttDigitalAdapterConfigurationBuilder builder(String brokerAddress, Integer brokerPort) throws MqttDigitalAdapterConfigurationException {
        return new MqttDigitalAdapterConfigurationBuilder(brokerAddress, brokerPort);
    }

    public static MqttDigitalAdapterConfigurationBuilder builder(String filepath) throws IOException {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        JsonNode jsonNode = yamlMapper.readTree(new File(filepath));
        return new  MqttDigitalAdapterConfigurationBuilder(jsonNode);
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public Integer getBrokerPort() {
        return brokerPort;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getClientId() {
        return clientId;
    }

    public String getBrokerConnectionString(){
        return String.format("tcp://%s:%d", brokerAddress, brokerPort);
    }

    public MqttClientPersistence getPersistence() {
        return persistence;
    }

    public MqttConnectOptions getConnectOptions(){
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(automaticReconnectFlag);
        options.setCleanSession(cleanSessionFlag);
        options.setConnectionTimeout(connectionTimeout);
        if(username != null && !username.isEmpty() && password != null && !password.isEmpty()){
            options.setUserName(username);
            options.setPassword(password.toCharArray());
        }
        return options;
    }

    protected void setConnectionTimeout(Integer connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    protected void setCleanSessionFlag(boolean cleanSession) {
        this.cleanSessionFlag = cleanSession;
    }

    protected void setAutomaticReconnectFlag(boolean automaticReconnect){
        this.automaticReconnectFlag = automaticReconnect;
    }

    protected void setMqttClientPersistence(MqttClientPersistence persistence) {
        this.persistence = persistence;
    }

    public Map<String, PropertyOutgoingTopic<?>> getPropertyUpdateTopics() {
        return propertyUpdateTopics;
    }

    public Map<String, EventNotificationOutgoingTopic<?>> getEventNotificationTopics() {
        return eventNotificationTopics;
    }

    public Map<String, ActionIncomingTopic<?>> getActionIncomingTopics() {
        return actionIncomingTopics;
    }
}
