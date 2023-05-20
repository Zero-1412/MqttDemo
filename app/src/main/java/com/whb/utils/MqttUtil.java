package com.whb.utils;

import android.content.Context;
import android.text.TextUtils;
import android.util.Log;
import android.widget.Toast;
import com.whb.connect.MqttConnection;
import com.whb.entity.AMessage;
import org.eclipse.paho.android.service.MqttAndroidClient;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttUtil {
    
    private static final String TAG = "MqttUtil";
    
    private static String mqttUsername;
    
    private static String mqttPassword;
    
    private static String clientId;
    
    private static MqttAndroidClient mqttAndroidClient;
    
    public static final LinkedBlockingQueue<String> CLIENT_QUEUE = new LinkedBlockingQueue<>(1000);
    
    public static final LinkedBlockingQueue<AMessage> SERVER_QUEUE = new LinkedBlockingQueue<>(
            1000);
    
    
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    private static final String topic = "/" + mqttUsername + "/" + clientId + "/user/get";
    
    static {
        executor.execute(new IotPublishRunnable());
    }
    
    public static void initIot(Context context) {
        
        String host = "";
        String port = "";
        
        MqttConnection connection = new MqttConnection(context, host, Integer.parseInt(port),
                clientId, mqttUsername, mqttPassword, false);
        mqttAndroidClient = connection.getMqttAndroidClient(context);
        try {
            mqttAndroidClient.connect(connection.getMqttConnectOptions(), null,
                    new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                            Log.i(TAG, "初始化成功");
                            
                            //这里订阅消息
                            subscribe(context);
                        }
                        
                        @Override
                        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                            Log.e(TAG, "初始化失败!" + exception.getMessage());
                        }
                    });
            
            mqttAndroidClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean b, String s) {
                    Log.i(TAG, "连接成功");
                    Toast.makeText(context, "连接成功", Toast.LENGTH_LONG).show();
                }
                
                @Override
                public void connectionLost(Throwable cause) {
                    Log.e(TAG, cause.getMessage());
                    Toast.makeText(context, "连接失败", Toast.LENGTH_LONG).show();
                }
                
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    try {
                        Log.i(TAG, "onNotify: " + topic);
                        if (TextUtils.isEmpty(topic)) {
                            return;
                        }
                        
                        AMessage aMessage = new AMessage();
                        aMessage.setData(message.getPayload());
                        boolean offer = SERVER_QUEUE.offer(aMessage);
                        if (!offer) {
                            Log.e(TAG, "队列已满，无法接受消息!");
                        }
                        
                    } catch (Exception e) {
                        Log.e(TAG, "接受消息处理失败");
                    }
                }
                
                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                
                }
            });
        } catch (Exception e) {
            Log.e(TAG, "INIT IOT ERROR!");
        }
    }
    
    /**
     * 建立连接
     * */
    private static void subscribe(Context context) {
        try {
            mqttAndroidClient.subscribe(topic, 1, null,
                    new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                            Log.i(TAG,
                                    "订阅成功 topic: "
                                            + topic);
                            Toast.makeText(context,"订阅成功", Toast.LENGTH_LONG).show();
                        }
                        
                        @Override
                        public void onFailure(IMqttToken asyncActionToken,
                                Throwable exception) {
                            Log.e(TAG, "订阅失败!" + exception.getMessage());
                            Toast.makeText(context,"订阅失败", Toast.LENGTH_LONG).show();
                        }
                    });
            
        } catch (Exception e) {
            Log.e(TAG, "订阅失败!" + e.getMessage());
            Toast.makeText(context,"订阅失败", Toast.LENGTH_LONG).show();
        }
    }
    
    /**
     * 断开连接
     */
    public static void disconnect(Context context) {
        if (null == mqttAndroidClient || !mqttAndroidClient.isConnected()) {
            Log.w(TAG, "IOT还未初始化！");
            return;
        }
        
        try {
            mqttAndroidClient.disconnect().setActionCallback(new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    Log.i(TAG, "断开连接成功!");
                    Toast.makeText(context,"断开连接成功", Toast.LENGTH_LONG).show();
                }
                
                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    Log.i(TAG, "断开连接失败!");
                    Toast.makeText(context,"断开连接失败", Toast.LENGTH_LONG).show();
                }
            });
        } catch (MqttException e) {
            Log.e(TAG, e.getMessage());
            Toast.makeText(context,"断开连接失败", Toast.LENGTH_LONG).show();
        }
    }
    
    /**
     * 取消顶端
     */
    public static void unsubscribe(Context context) {
        if (null == mqttAndroidClient || !mqttAndroidClient.isConnected()) {
            Log.w(TAG, "IOT还未初始化！");
            return;
        }
        
        try {
            mqttAndroidClient.unsubscribe(topic).setActionCallback(new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    Log.i(TAG, "退订成功");
                    Toast.makeText(context,"退订成功", Toast.LENGTH_LONG).show();
                }
                
                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    Log.i(TAG, "退订失败");
                    Toast.makeText(context,"退订失败", Toast.LENGTH_LONG).show();
                }
            });
        } catch (MqttException e) {
            Log.e(TAG, e.getMessage());
            Toast.makeText(context,"退订失败", Toast.LENGTH_LONG).show();
        }
    }
    
    static class IotPublishRunnable implements Runnable {
        
        @Override
        public void run() {
            while (true) {
                try {
                    String msg = CLIENT_QUEUE.take();
                    if (TextUtils.isEmpty(msg)) {
                        continue;
                    }
                    publish(msg);
                    Thread.sleep(300);
                } catch (Exception e) {
                    Log.e(TAG, "处理iot消息失败");
                }
                
            }
        }
    }
    
    public static boolean publish(final String payload) {
        if (TextUtils.isEmpty(mqttUsername) || TextUtils.isEmpty(mqttPassword) || TextUtils.isEmpty(
                clientId)) {
            return false;
        }
        publishNew(payload);
        return true;
    }
    
    private static void publishNew(final String payload) {
        String topic = "/" + mqttUsername + "/" + clientId + "/user/update";
        Integer qos = 1;
        
        try {
            if (null == mqttAndroidClient || !mqttAndroidClient.isConnected()) {
                Log.w(TAG, "IOT还未初始化！无法发送消息");
                return;
            }
            mqttAndroidClient.publish(topic, payload.getBytes(StandardCharsets.UTF_8), qos, false,
                    null, new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                        
                        }
                        
                        @Override
                        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                            String[] topics = asyncActionToken.getTopics();
                            Log.e(TAG, "publish message error! topics: " + Arrays.toString(topics));
                        }
                    });
        } catch (MqttException e) {
            Log.e(TAG, "发送消息失败！");
        } catch (IllegalArgumentException e) {
            Log.e(TAG, "MQTT CLIENT ERROR");
        }
    }
    
    public static void putQueue(String msg) {
        boolean offer = CLIENT_QUEUE.offer(msg);
        if (!offer) {
            Log.w(TAG, "操作队列已满！");
        }
    }
}
