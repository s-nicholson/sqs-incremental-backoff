package com.example.sqsbackoff;

import org.springframework.stereotype.Service;

import java.util.Map;

import static com.example.sqsbackoff.SqsBackoffService.SqsBackoffException;

@Service
public class MessageProcessor {
    void processBusiness(Map<String, Object> payload) throws SqsBackoffException {
        Object special = payload.get("specialRetry");
        if (Boolean.TRUE.equals(special)) {
            System.out.println("Throwing special retry");
            throw new SqsBackoffException("triggered special retry");
        }
        System.out.println("Success!");
    }
}
