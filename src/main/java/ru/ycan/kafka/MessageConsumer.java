package ru.ycan.kafka;

public interface MessageConsumer {
    void startReadMessages(String topic);
}
