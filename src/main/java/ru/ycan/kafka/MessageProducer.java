package ru.ycan.kafka;

/**
 * Интерфейс для работы с продюссером kafka.
 */
public interface MessageProducer {
    /**
     * Метод для отправки сообщения в kafka.
     *
     * @param topic      топик в kafka
     * @param keyMessage ключ сообщения в kafka
     * @param message    сообщение отправляемое в kafka
     * @param <T>        принимаемый тип сообщения
     */
    <T> void sentMessage(String topic, String keyMessage, T message);
}
