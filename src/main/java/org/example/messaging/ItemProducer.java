package org.example.messaging;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.example.models.Item;

import javax.jms.*;

public class ItemProducer implements AutoCloseable {
    private String baseUrl;

    private ConnectionFactory factory;

    private Connection connection;

    private Session session;

    private Queue queue;

    private MessageProducer producer;

    private boolean inError;

    private String error;

    private static ItemProducer instance;

    public static void init(String baseUrl, String queName) {
        instance = new ItemProducer(baseUrl, queName);
    }

    public static boolean inError() {
        return instance.inError;
    }

    public static String getError() {
        return instance.error;
    }

    public static void add(Item item) {
        instance.publish(item);
    }

    public static void terminate() {
        instance.close();
    }

    private ItemProducer(String baseUrl, String queName) {
        this.baseUrl = baseUrl;

        factory = new ActiveMQConnectionFactory(baseUrl);

        try {
            connection = factory.createConnection();
            connection.start();
        } catch (Exception ex) {
            inError = true;
            error = ex.getMessage();
            return;
        }

        try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (Exception ex) {
            inError = true;
            error = ex.getMessage();
            return;
        }

        try {
            queue = session.createQueue(queName);
        } catch (Exception ex) {
            inError = true;
            error = ex.getMessage();
            return;
        }

        try {
            producer = session.createProducer(queue);
        } catch (Exception ex) {
            inError = true;
            error = ex.getMessage();
        }
    }

    private void publish(Item item) {
        try {
            var msg = session.createObjectMessage(item.id);
            msg.setStringProperty("title", item.title);
            msg.setIntProperty("id", item.id);
            producer.send(msg);
        } catch (Exception ex) {
            inError = true;
            error = ex.getMessage();
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
            session.close();
            connection.close();
        } catch (Exception ignored) {}
    }
}
