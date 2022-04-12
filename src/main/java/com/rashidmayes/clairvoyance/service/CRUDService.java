package com.rashidmayes.clairvoyance.service;

import com.aerospike.client.*;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.rashidmayes.clairvoyance.App;
import com.rashidmayes.clairvoyance.interfaces.FunctionT1;
import com.rashidmayes.clairvoyance.interfaces.FunctionT2;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CRUDService {
    public static CRUDService crudServiceInstance = null;

    static {
        try {
            crudServiceInstance = new CRUDService();
        } catch (Exception e) {
            App.APP_LOGGER.severe("cannot instantiated CRUDService");
            e.printStackTrace();
        }
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();

    public CRUDService() {
        try {
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        } catch (Exception exception) {
            App.APP_LOGGER.severe("json setting error: " + exception.getMessage());
            exception.printStackTrace();
        }
    }

    public void delete(String namespace, String setName, String user_key, FunctionT2<Key, Boolean> callbackOnSuccess, FunctionT1<Exception> callbackOnFailure) {
        Key key = new Key(namespace, setName, user_key);
        delete(key, callbackOnSuccess, callbackOnFailure);
    }

    public void delete(Key key, FunctionT2<Key, Boolean> callbackOnSuccess, FunctionT1<Exception> callbackOnFailure) {
        AerospikeClient client = App.getClient();
        // client.delete(null, new AsyncDeleteListener(callbackOnSuccess, callbackOnFailure), client.writePolicyDefault, new Key(namespace, setName, Value.get(user_key)));
        callbackOnSuccess.apply(
            key,
            client.delete(client.writePolicyDefault, key)
        );
    }

    public void delete(String namespace, String setName, String key) {
        this.delete(namespace, setName, key, null, null);
    }

    public void save(String namespace, String setName, String value, FunctionT1<Key> callbackOnSuccess, FunctionT1<Exception> callbackOnFailure) {
        ConcurrentHashMap<String, Object> concurrentHashMap;
        try {
            concurrentHashMap = objectMapper.readValue(value, ConcurrentHashMap.class);
        } catch (Exception exception) {
            App.APP_LOGGER.severe("json parse error: " + exception.getMessage());
            exception.printStackTrace();
            return;
        }
        save(namespace, setName, concurrentHashMap, callbackOnSuccess, callbackOnFailure);
    }

    public void save(String namespace, String setName, Map<String, Object> value, FunctionT1<Key> callbackOnSuccess, FunctionT1<Exception> callbackOnFailure) throws IllegalArgumentException, AerospikeException {
        AerospikeClient client = App.getClient();
        if (namespace.trim().equals("")) {
            App.APP_LOGGER.severe("document has no namespace");
            throw new IllegalArgumentException("document has no namespace");
        } else if (setName.trim().equals("")) {
            App.APP_LOGGER.severe("document has no setName");
            throw new IllegalArgumentException("document has no setName");
        } else if (!value.containsKey("@user_key") && !value.containsKey("id")) {
            App.APP_LOGGER.severe("document has no @user_key or id");
            throw new IllegalArgumentException("document has no id @user_key or id");
        }
        Key key = new Key(namespace, setName, value.getOrDefault("@user_key", value.get("id")).toString());
        // client.put(null, new AsyncWriteListener(callbackOnSuccess, callbackOnFailure), client.writePolicyDefault, new Key(namespace, setName, new Value.StringValue(value.getOrDefault("@user_key", value.get("id")).toString())), getBin(value));
        client.put(client.writePolicyDefault, key, getBin(value));
        callbackOnSuccess.apply(key);

    }

    public Bin[] getBin(Map<String, Object> value) {
        ArrayList<Bin> binsList = new ArrayList();
        for (String key : value.keySet()) {
            binsList.add(new Bin(key, value.get(key)));
        }
        Bin[] binsArray = new Bin[binsList.size()];
        binsList.toArray(binsArray);
        return binsArray;
    }

    private class AsyncDeleteListener implements DeleteListener {
        private final FunctionT2<Key, Boolean> callbackOnSuccess;
        private final FunctionT1<Exception> callbackOnFailure;

        public AsyncDeleteListener(FunctionT2<Key, Boolean> callbackOnSuccess, FunctionT1<Exception> callbackOnFailure) {
            this.callbackOnSuccess = callbackOnSuccess;
            this.callbackOnFailure = callbackOnFailure;
        }

        @Override
        public void onSuccess(Key key, boolean b) {
            if (this.callbackOnSuccess != null) {
                this.callbackOnSuccess.apply(key, b);
            }
        }

        @Override
        public void onFailure(AerospikeException e) {
            e.printStackTrace();
            if (this.callbackOnFailure != null) {
                this.callbackOnFailure.apply(e);
            }
        }
    }

    private class AsyncWriteListener implements WriteListener {
        private final FunctionT1<Key> callbackOnSuccess;
        private final FunctionT1<Exception> callbackOnFailure;

        public AsyncWriteListener(FunctionT1<Key> callbackOnSuccess, FunctionT1<Exception> callbackOnFailure) {
            this.callbackOnSuccess = callbackOnSuccess;
            this.callbackOnFailure = callbackOnFailure;
        }

        @Override
        public void onSuccess(Key key) {
            if (this.callbackOnSuccess != null) {
                this.callbackOnSuccess.apply(key);
            }
        }

        @Override
        public void onFailure(AerospikeException e) {
            if (this.callbackOnFailure != null) {
                this.callbackOnFailure.apply(e);
            }
        }
    }

}
