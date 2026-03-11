package ru.ycan.kafka.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.ycan.kafka.MessageProducer;
import ru.ycan.utils.bson.BsonMapper;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class MessageProducerImpl implements MessageProducer {
    // Создание продюсера
    private static final KafkaProducer<String, byte[]> PRODUCER = new KafkaProducer<>(getProducerProperties());

    @Override
    public <T> void sentMessage(String topic, String keyMessage, T message) {
        // Создание продюсера
        try {
            // Сериализация сообщения
            log.info("Сообщение до сериализации: {}", message);
            var value = BsonMapper.writeValueAsBytes(message);
            log.info("Сообщение до отправки в kafka: {}", Arrays.toString(value));
            var record = new ProducerRecord<>(topic, keyMessage, value);
            log.info("ProducerRecord до отправки в kafka: {}", record);
            // Отправка сообщения
            PRODUCER.send(record);
            log.info("Сообщение отправлено в топик '{}'", topic);
        } catch (JsonProcessingException e) {
            log.error("Не удалось сериализовать объект: {}", e.getMessage(), e);
        }
    }

    /**
     * Отдельный метод получения свойств для подключения к {@link KafkaProducer}
     * @return свойства {@link Properties} для подключения к {@link KafkaProducer}
     */
    private static Properties getProducerProperties() {
        // Конфигурация продюсера – адрес сервера, сериализаторы для ключа и значения, .
        Properties props = new Properties();
        // Адреса брокеров Kafka для локального прогона
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        // Адреса брокеров Kafka для docker
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:29092,kafka2:29093,kafka3:29094");
        // Сериализация ключа
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Сериализация значения
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Подтверждение от брокеров. all - ждёт от всех (не только от лидера)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Кол-во повторных попыток отправки сообщения
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        return props;
    }
}
