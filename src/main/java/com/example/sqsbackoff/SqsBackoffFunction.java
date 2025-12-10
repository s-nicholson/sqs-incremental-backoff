package com.example.sqsbackoff;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.example.sqsbackoff.SqsBackoffService.SqsBackoffException;
import com.example.sqsbackoff.model.Message;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.*;

@SpringBootApplication
public class SqsBackoffFunction {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean("sqsHandler")
    public Function<SQSEvent, SQSBatchResponse> handler(MessageProcessor messageProcessor, SqsBackoffService sqsBackoffService) {
        return event -> {
            System.out.println("Lambda start");
            List<BatchItemFailure> failures = new ArrayList<>();

            for (SQSMessage sqsMessage : event.getRecords()) {
                var body = sqsMessage.getBody();
                System.out.println("Processing message: " + body);
                try {
                    Message<Map<String, Object>> message = objectMapper.readValue(body, new TypeReference<>() {
                    });
                    System.out.println("Reading message: " + message);

                    messageProcessor.processBusiness(message.getPayload());
                } catch (SqsBackoffException sre) {
                    var retry = sqsBackoffService.backoff(sqsMessage);
                    if (retry.retried()) {
                        System.out.println("Retrying attempt: " + retry.attempt() +
                                " after: " + retry.newVisibilityTimeout());
                        failures.add(new BatchItemFailure(sqsMessage.getMessageId()));
                    } else {
                        System.out.println("Retries expired after attempt: " + retry.attempt());
                    }
                } catch (Exception e) {
                    System.out.println("Exception");
                    failures.add(new BatchItemFailure(sqsMessage.getMessageId()));
                }
            }

            System.out.println("Lambda done: " + failures);
            return new SQSBatchResponse(failures);
        };
    }

    public static void main(String[] args) {
    }
}
