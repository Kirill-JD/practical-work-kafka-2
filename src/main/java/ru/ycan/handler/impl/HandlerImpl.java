package ru.ycan.handler.impl;

import lombok.extern.slf4j.Slf4j;
import ru.ycan.handler.Handler;
import ru.ycan.kafka.MessageConsumer;
import ru.ycan.kafka.MessageProducer;
import ru.ycan.kafka.impl.BatchMessageConsumer;
import ru.ycan.kafka.impl.MessageProducerImpl;
import ru.ycan.kafka.impl.SingleMessageConsumer;
import ru.ycan.kafka.message.Notification;
import ru.ycan.kafka.message.RecipientGroup;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static ru.ycan.kafka.message.RecipientGroup.CUSTOMERS;
import static ru.ycan.kafka.message.RecipientGroup.EMPLOYEES;

@Slf4j
public class HandlerImpl implements Handler {
    private static final String TOPIC = "notification-topic";
    private static final String PREFIX_KEY = "group_";
    private static final int MESSAGES_COUNT = 200;
    private static final MessageProducer MESSAGE_PRODUCER = new MessageProducerImpl();
    private static final MessageConsumer SINGLE_MESSAGE_CONSUMER = new SingleMessageConsumer();
    private static final MessageConsumer BATCH_MESSAGE_CONSUMER = new BatchMessageConsumer();

    @Override
    public void startProcess() {
        readMessagesProcess();
        sendMessagesProcess();
    }

    /**
     * Запуск консьюмеров в отдельных потоках
     */
    private void readMessagesProcess() {
        new Thread(() -> SINGLE_MESSAGE_CONSUMER.startReadMessages(TOPIC)).start();
        new Thread(() -> BATCH_MESSAGE_CONSUMER.startReadMessages(TOPIC)).start();
    }

    /**
     * Запуск продюсера.
     * Отправляет 200 сообщений с задержкой между отправками (иначе {@link BatchMessageConsumer} вычитает все 200 сообщений разом).
     */
    private void sendMessagesProcess() {
        for (int i = 0; i < MESSAGES_COUNT; i++) {
            var uuid = UUID.randomUUID().toString();
            if (i % 2 == 0) {
                MESSAGE_PRODUCER.sentMessage(TOPIC, getKeyMessage(EMPLOYEES), getNotification(uuid, EMPLOYEES));
            } else {
                MESSAGE_PRODUCER.sentMessage(TOPIC, getKeyMessage(CUSTOMERS), getNotification(uuid, CUSTOMERS));
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                log.warn("Не удалось сделать задержку между отправками");
            }
        }
        log.info("Отправлено '{}' сообщений", MESSAGES_COUNT);
    }

    /**
     * Создание тестового сообщения для отправки в kafka.
     *
     * @param group группа пользователей
     * @return сообщение {@link Notification}
     */
    private Notification getNotification(String uuid, RecipientGroup group) {
        return Notification.builder()
                           .uuid(uuid)
                           .recipientGroup(group)
                           .message("Тестовое сообщение для группы '%s'.".formatted(group.getValue()))
                           .dateOfDispatch(Instant.now().plus(Duration.ofDays(1)).toEpochMilli())
                           .build();
    }

    /**
     * Метод для получения ключа сообщения.
     *
     * @param group группа пользователей из сообщения
     * @return строку в формате 'group_<название группы>'
     */
    private String getKeyMessage(RecipientGroup group) {
        return PREFIX_KEY.concat(group.name().toLowerCase());
    }
}
