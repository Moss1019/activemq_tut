package org.example.messaging;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.example.models.Item;

import javax.jms.*;
import java.util.function.Consumer;

public class ItemConsumer implements AutoCloseable {
    private String baseUrl;

    private ConnectionFactory factory;

    private Connection connection;

    private Session session;

    private Queue queue;

    private MessageConsumer consumer;

    private boolean isRunning;

    private boolean inError;

    private String error;

    private Thread worker;

    private Consumer<Item> itemHandler;

    private static ItemConsumer instance;

    public static void init(String baseUrl, String queName, Consumer<Item> itemHandler) {
        instance = new ItemConsumer(baseUrl, queName, itemHandler);
    }

    public static boolean inError() {
        return instance.inError;
    }

    public static String getError() {
        return instance.error;
    }

    public static void terminate() {
        instance.close();
    }

    private ItemConsumer(String baseUrl, String queName, Consumer<Item> itemHandler) {
        this.baseUrl = baseUrl;
        this.itemHandler = itemHandler;

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
            consumer = session.createConsumer(queue);
        } catch (Exception ex) {
            inError = true;
            error = ex.getMessage();
            return;

        }

        worker = new Thread(this::doWork);
        isRunning = true;
        worker.start();
    }

    private void doWork() {
        while(isRunning) {
            try {
                var msg = (ObjectMessage)consumer.receive();
                var item = new Item(msg.getIntProperty("id"), msg.getStringProperty("title"));
                itemHandler.accept(item);
            } catch (Exception ex) {
                inError = true;
                error = ex.getMessage();
            }
        }
    }

    @Override
    public void close() {
        isRunning = false;
        try {
            consumer.close();
            session.close();
            connection.close();
        } catch (Exception ignored) {}
        try {
            worker.join();
        } catch (Exception ignored) {

        }
    }
}
