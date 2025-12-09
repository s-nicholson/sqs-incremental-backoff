package com.example.sqsbackoff.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Jacksonized
@Builder
@Data
public class Message<T> {
    @JsonProperty("payload")
    private T payload;
}
