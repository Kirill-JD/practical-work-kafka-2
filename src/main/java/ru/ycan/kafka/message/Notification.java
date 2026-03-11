package ru.ycan.kafka.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

/**
 * Сообщение для передачи/получения в kafka (по сути просто рандомные поля для задания).
 *
 * @param recipientGroup группа пользователей для дальнейшей обработки
 * @param message        текстовое сообщение для передачи определённой группе пользователей
 * @param dateOfDispatch дата в формате Unix timestamp для отправки пользователей
 */
@Builder
public record Notification(@JsonProperty(value = "uuid", required = true) String uuid,
                           @JsonProperty(value = "recipient_group", required = true) RecipientGroup recipientGroup,
                           @JsonProperty(value = "message", required = true) String message,
                           @JsonProperty(value = "date_of_dispatch", required = true) long dateOfDispatch) {
}
