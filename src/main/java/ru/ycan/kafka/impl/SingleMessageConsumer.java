package ru.ycan.kafka.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.ycan.kafka.MessageConsumer;
import ru.ycan.kafka.message.Notification;
import ru.ycan.utils.bson.BsonMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class SingleMessageConsumer implements MessageConsumer {

    @Override
    public void startReadMessages(String topic) {
        var props = getConsumerProperties();
        try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
            // Подписка на топик
            consumer.subscribe(Collections.singletonList(topic));
            // Чтение сообщений в бесконечном цикле
            while (true) {
                try {
                    // Получение сообщений
                    var records = consumer.poll(Duration.ofMillis(100));
                    for (var record : records) {
                        deserializerValue(record, Notification.class, topic);
                    }
                } catch (Exception e) {
                    log.error("Не удалось вычитать/обработать сообщение", e);
                }
            }
        }
    }

    private <T> void deserializerValue(ConsumerRecord<String, byte[]> record, Class<T> clazz, String topic) {
        // логируем полученное сообщение
        log.info("Получено сообщение: key = {}, partition = {}, offset = {}",
                 record.key(), record.partition(), record.offset());
        try {
            var value = BsonMapper.readValue(record.value(), clazz);
            log.info("Десериализованное сообщение из топика '{}': {}", topic, value);
        } catch (IOException e) {
            log.error("Ошибка при десериализации сообщения из топика '{}'", topic, e);
        }
    }

    private Properties getConsumerProperties() {
        Properties props = new Properties();
        // Адреса брокеров Kafka для локального прогона
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        // Адреса брокеров Kafka для docker
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:29092,kafka2:29093,kafka3:29094");
        // Уникальный идентификатор группы
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-consumer-group");
        // Десериализация ключа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Десериализация значения
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Начало чтения с самого начала
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Автоматический коммит смещений
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // Время ожидания активности от консьюмера
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        return props;
    }
}
