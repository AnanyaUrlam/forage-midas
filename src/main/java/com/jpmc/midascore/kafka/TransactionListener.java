package com.jpmc.midascore.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.foundation.Transaction;

import java.util.ArrayList;
import java.util.List;

@Component
public class TransactionListener {

    private final List<Float> firstFourAmounts = new ArrayList<>();
    private boolean outputPrinted = false; // ✅ flag to prevent repeat printing

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        if (firstFourAmounts.size() < 4) {
            firstFourAmounts.add(transaction.getAmount());
        }

        if (firstFourAmounts.size() == 4 && !outputPrinted) {
            System.out.println("---begin output---");
            System.out.println(String.join(",", firstFourAmounts.stream()
                    .map(amount -> String.format("%.2f", amount))
                    .toArray(String[]::new)));
            System.out.println("---end output---");

            outputPrinted = true; // ✅ set flag so it doesn't print again
        }
    }
}
