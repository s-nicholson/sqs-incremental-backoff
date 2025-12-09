package com.example.sqsbackoff;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.example.sqsbackoff.model.Message;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.StandardException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@SpringBootApplication
public class SqsBackoffFunction {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SqsClient sqs = SqsClient.create();
    private final String queueUrl = System.getenv("QUEUE_URL");

    /**
     * 10 mins, 20 mins, 30 mins
     */
    private final int[] RETRY_SCHEDULE_SECONDS = {600, 1200, 1800};

    /**
     * 12 hours
     */
    private static final int SQS_MAX_VISIBILITY_SECONDS = 43200;

    @Bean("sqsHandler")
    public Function<SQSEvent, SQSBatchResponse> handler() {
        return event -> {
            System.out.println("Lambda start");
            List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();

            for (SQSMessage sqsMessage : event.getRecords()) {
                System.out.println("Processing message");
                try {
                    Message<Map<String, Object>> message = objectMapper.readValue(
                            sqsMessage.getBody(),
                            new TypeReference<>() {
                            }
                    );
                    System.out.println("Reading message: " + message);

                    processBusiness(message.getPayload());
                } catch (SqsBackoffException sre) {
                    System.out.println("SqsBackoffException");
                    int attempt = extractAttempt(sqsMessage);
                    int delay = computeDelaySeconds(attempt);
                    changeVisibility(sqsMessage.getReceiptHandle(), delay);

                    failures.add(new SQSBatchResponse.BatchItemFailure(sqsMessage.getMessageId()));

                } catch (Exception e) {
                    System.out.println("Exception");
                    failures.add(new SQSBatchResponse.BatchItemFailure(sqsMessage.getMessageId()));
                }
            }

            System.out.println("Lambda done");
            return new SQSBatchResponse(failures);
        };
    }

    private void processBusiness(Map<String, Object> payload) throws SqsBackoffException {
        Object special = payload.get("specialRetry");
        if (special != null && Boolean.TRUE.equals(special)) {
            System.out.println("Throwing special retry");
            throw new SqsBackoffException("triggered special retry");
        }
        System.out.println("Success!");
    }

    private int extractAttempt(SQSMessage msg) {
        String recvCount = msg.getAttributes().get("ApproximateReceiveCount");
        try {
            int rc = Integer.parseInt(recvCount);
            return Math.max(0, rc - 1); // convert to 0-based attempt index
        } catch (Exception e) {
            return 0;
        }
    }


    private int computeDelaySeconds(int attempt) {
        int idx = Math.min(attempt, RETRY_SCHEDULE_SECONDS.length - 1);
        int delay = RETRY_SCHEDULE_SECONDS[idx];
        if (delay > SQS_MAX_VISIBILITY_SECONDS) delay = SQS_MAX_VISIBILITY_SECONDS;
        return delay;
    }

    private void changeVisibility(String receiptHandle, int delaySeconds) {
        ChangeMessageVisibilityRequest req = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .visibilityTimeout(delaySeconds)
                .build();

        sqs.changeMessageVisibility(req);
    }

    @StandardException
    public static class SqsBackoffException extends Exception {
    }

    public static void main(String[] args) {
    }
}
