package org.example;

import org.example.messaging.ItemConsumer;
import org.example.messaging.ItemProducer;
import org.example.models.Item;

public class Program {
    public static void main(String[] args) {
        var isRunning = true;
        var baseUrl = "tcp://127.0.0.1:61616";
        var queName = "queItems";
        ItemProducer.init(baseUrl, queName);
        ItemConsumer.init(baseUrl, queName, item -> {
            System.out.printf("%d - %s%n", item.id, item.title);
        });

        if(ItemProducer.inError()) {
            System.out.println(ItemProducer.getError());
        }
        if(ItemConsumer.inError()) {
            System.out.println(ItemConsumer.getError());
        }

        while(isRunning) {
            var input = getInput();
            if(input.equals("-q")) {
                isRunning = false;
            } else {
                var item = new Item(input);
                ItemProducer.add(item);
            }
        }

        ItemConsumer.terminate();
        ItemProducer.terminate();
    }

    public static String getInput() {
        try {
            var buffer = new byte[32];
            var read = System.in.read(buffer);
            return new String(buffer, 0, read - 1);
        } catch (Exception ex) {
            return "";
        }
    }
}
