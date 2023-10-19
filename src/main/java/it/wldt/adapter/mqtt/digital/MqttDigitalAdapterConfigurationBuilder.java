package it.wldt.adapter.mqtt.digital;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import it.wldt.adapter.mqtt.digital.exception.MqttDigitalAdapterConfigurationException;
import it.wldt.adapter.mqtt.digital.topic.MqttQosLevel;
import it.wldt.adapter.mqtt.digital.topic.incoming.ActionIncomingTopic;
import it.wldt.adapter.mqtt.digital.topic.outgoing.EventNotificationOutgoingTopic;
import it.wldt.adapter.mqtt.digital.topic.outgoing.PropertyOutgoingTopic;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class MqttDigitalAdapterConfigurationBuilder {

    private final MqttDigitalAdapterConfiguration configuration;
    private JsonNode configFileContent;

    public MqttDigitalAdapterConfigurationBuilder(String brokerAddress, Integer brokerPort) throws MqttDigitalAdapterConfigurationException {
        if(!isValid(brokerAddress) || isValid(brokerPort))
            throw new MqttDigitalAdapterConfigurationException("Broker Address cannot be empty string or null and Broker Port must be a positive number");
        configuration = new MqttDigitalAdapterConfiguration(brokerAddress, brokerPort);
    }

    public MqttDigitalAdapterConfigurationBuilder(String brokerAddress, Integer brokerPort, String clientId) throws MqttDigitalAdapterConfigurationException {
        if(!isValid(brokerAddress) || isValid(brokerPort) || !isValid(clientId))
            throw new MqttDigitalAdapterConfigurationException("Broker Address and Client Id cannot be empty string or null and Broker Port must be a positive number");
        configuration = new MqttDigitalAdapterConfiguration(brokerAddress, brokerPort);
    }

    public MqttDigitalAdapterConfigurationBuilder(JsonNode fileContent){
        configFileContent = fileContent;
        configuration = new MqttDigitalAdapterConfiguration(getBrokerAddress(), getBrokerPort());
    }

    public <T> MqttDigitalAdapterConfigurationBuilder addPropertyTopic(String propertyKey,
                                                                       String topic,
                                                                       MqttQosLevel qosLevel,
                                                                       Function<T, String> propertyToPayloadFunction) throws MqttDigitalAdapterConfigurationException {
        checkTopic(propertyKey, topic, propertyToPayloadFunction);
        this.configuration.getPropertyUpdateTopics().put(propertyKey, new PropertyOutgoingTopic<>(topic, qosLevel, propertyToPayloadFunction));
        return this;
    }

    public <T> MqttDigitalAdapterConfigurationBuilder addEventNotificationTopic(String eventKey,
                                                                                String topic,
                                                                                MqttQosLevel qosLevel,
                                                                                Function<T, String> eventToPayloadFunction) throws  MqttDigitalAdapterConfigurationException{
        checkTopic(eventKey, topic, eventToPayloadFunction);
        this.configuration.getEventNotificationTopics().put(eventKey, new EventNotificationOutgoingTopic<>(topic, qosLevel, eventToPayloadFunction));
        return this;
    }

    public <T> MqttDigitalAdapterConfigurationBuilder addActionTopic(String actionKey,
                                                                     String topic,
                                                                     Function<String, T> payloadToActionFunction) throws MqttDigitalAdapterConfigurationException {
        checkTopic(actionKey, topic, payloadToActionFunction);
        this.configuration.getActionIncomingTopics().put(actionKey, new ActionIncomingTopic<>(topic, actionKey, payloadToActionFunction));
        return this;
    }

    public MqttDigitalAdapterConfigurationBuilder setConnectionTimeout(Integer connectionTimeout) throws MqttDigitalAdapterConfigurationException {
        if(isValid(connectionTimeout)) throw new MqttDigitalAdapterConfigurationException("Connection Timeout must be a positive number");
        this.configuration.setConnectionTimeout(connectionTimeout);
        return this;
    }

    public MqttDigitalAdapterConfigurationBuilder setCleanSessionFlag(boolean cleanSession) {
        this.configuration.setCleanSessionFlag(cleanSession);
        return this;
    }

    public MqttDigitalAdapterConfigurationBuilder setAutomaticReconnectFlag(boolean automaticReconnect){
        this.configuration.setAutomaticReconnectFlag(automaticReconnect);
        return this;
    }

    public MqttDigitalAdapterConfigurationBuilder setMqttClientPersistence(MqttClientPersistence persistence) throws MqttDigitalAdapterConfigurationException {
        if(persistence == null) throw new MqttDigitalAdapterConfigurationException("MqttClientPersistence cannot be null");
        this.configuration.setMqttClientPersistence(persistence);
        return this;
    }

    public MqttDigitalAdapterConfiguration build() throws MqttDigitalAdapterConfigurationException {
        if(this.configuration.getActionIncomingTopics().isEmpty()
                && this.configuration.getEventNotificationTopics().isEmpty()
                && this.configuration.getPropertyUpdateTopics().isEmpty())
            throw new MqttDigitalAdapterConfigurationException("Cannot build a MqttDigitalAdapterConfiguration without MqttTopics");

        return this.configuration;
    }

    private <I, O> void checkTopic(String key, String topic, Function<I, O> function) throws MqttDigitalAdapterConfigurationException {
        if(!isValid(key) || !isValid(topic) || function == null)
            throw new MqttDigitalAdapterConfigurationException("Key and Topic cannot be empty or null and function cannot be null");
    }

    private boolean isValid(String param){
        return param != null && !param.isEmpty();
    }

    private boolean isValid(int param){
        return param <= 0;
    }

    private String getBrokerAddress() {
        return configFileContent.get("brokerAddress").asText();
    }

    private int getBrokerPort() {
        return configFileContent.get("brokerPort").asInt();
    }

    public MqttDigitalAdapterConfigurationBuilder readFromConfig() throws MqttDigitalAdapterConfigurationException, IOException {
        JsonNode properties = configFileContent.get("daProperties");
        JsonNode actions = configFileContent.get("daActions");
        JsonNode events = configFileContent.get("daEvents");
        for (JsonNode p :properties) {
            addProperty(p);
        }
        for (JsonNode a :actions) {
            addAction(a);
        }
        for (JsonNode e :events) {
            addEvent(e);
        }

        return this;
    }


    private void addProperty(JsonNode p) throws MqttDigitalAdapterConfigurationException {
        String propertyKey = p.get("propertyKey").asText();
        String topic = p.get("topic").asText();
        //String type = p.get("type").asText();
        //String initialValue = p.get("initialValue").toString();
        addPropertyTopic(propertyKey, topic, MqttQosLevel.MQTT_QOS_0, value -> String.valueOf(((Double)value).intValue()));
        /*if ("int".equals(type)) {
            addPropertyTopic(propertyKey, topic, s -> Integer.valueOf(s));
        }
        else if ("double".equals(type) || "float".equals(type)) {
            addDigitalAssetPropertyAndTopic(propertyKey, Double.valueOf(initialValue), topic, s -> Double.valueOf(s));
        }
        else if ("boolean".equals(type)) {
            addDigitalAssetPropertyAndTopic(propertyKey, Boolean.valueOf(initialValue), topic, s -> Boolean.valueOf(s));
        }
        else if ("string".equals(type)) {
            addDigitalAssetPropertyAndTopic(propertyKey, String.valueOf(initialValue), topic, s -> String.valueOf(s));
        }
        else if ("json-array".equals(type)) {
            addJsonArrayProperty(p.get("field-type").asText(), propertyKey, initialValue, topic);
        }
        else if ("json-object".equals(type)) {
            addJsonObjectProperty(propertyKey, initialValue, topic);
        }*/
    }

    /*private void addJsonArrayProperty(String fieldType, String propertyKey, String initialValue, String topic) throws MqttDigitalAdapterConfigurationException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode initialValuesArray = null;
        try {
            initialValuesArray = objectMapper.readTree(initialValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
        addDigitalAssetPropertyAndTopic(propertyKey, (ArrayNode) initialValuesArray, topic, s -> {
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            try {
                List<JsonNode> values = objectMapper.readValue(s, typeFactory
                        .constructCollectionType(List.class, JsonNode.class));
                ArrayNode parsedList = objectMapper.createArrayNode();
                for (JsonNode element : values) {
                    if ("int".equals(fieldType)) {
                        parsedList.add(Integer.valueOf(element.asText()));
                    } else if ("double".equals(fieldType) || "float".equals(fieldType)) {
                        parsedList.add(Double.valueOf(element.asText()));
                    } else if ("boolean".equals(fieldType)) {
                        parsedList.add(Boolean.valueOf(element.asText()));
                    } else if ("string".equals(fieldType)) {
                        parsedList.add(String.valueOf(element.asText()));
                    } else {
                        parsedList.add(element);
                    }
                }
                return parsedList;
            } catch (JsonProcessingException e){
                e.printStackTrace();
                return null;
            }
        });
    }

    private void addJsonObjectProperty(String propertyKey, String initialValue, String topic) throws MqttDigitalAdapterConfigurationException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode initialValuesObject = objectMapper.createObjectNode();
        try {
            initialValuesObject = objectMapper.readValue(initialValue, ObjectNode.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        addDigitalAssetPropertyAndTopic(propertyKey, initialValuesObject, topic, s -> {
            ObjectNode parsedValues = objectMapper.createObjectNode();
            try {
                parsedValues = objectMapper.readValue(s, ObjectNode.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return parsedValues;
        });
    }*/

    private void addAction(JsonNode action) throws MqttDigitalAdapterConfigurationException {
        String actionKey = action.get("actionKey").asText();
        //String type = action.get("type").asText();
        //String contentType = action.get("contentType").asText();
        String topic = action.get("topic").asText();
        //String actionWord = action.get("action").asText();
        addActionTopic(actionKey, topic, msg -> "OFF");
        //addDigitalAssetActionAndTopic(actionKey, type, contentType, topic, actionBody -> actionWord + actionBody);

    }

    private void addEvent(JsonNode e) throws MqttDigitalAdapterConfigurationException {
        String eventKey = e.get("eventKey").asText();
        //String type = e.get("type").asText();
        String topic = e.get("topic").asText();
        addEventNotificationTopic(eventKey, topic, MqttQosLevel.MQTT_QOS_0, Object::toString);
        //addDigitalAssetEventAndTopic(eventKey, type, topic, Function.identity());
    }
}
