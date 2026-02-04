package ru.ycan.kafka.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Enum с возможными группами пользователей для дальнейшей обработки.
 */
@Getter
@RequiredArgsConstructor
public enum RecipientGroup {
    @JsonProperty("customers")
    CUSTOMERS("Клиенты"),
    @JsonProperty("employees")
    EMPLOYEES("Сотрудники");

    private final String value;
}
