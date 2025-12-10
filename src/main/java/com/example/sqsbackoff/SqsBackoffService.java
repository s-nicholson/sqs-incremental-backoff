package com.example.sqsbackoff;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.experimental.StandardException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;

import java.util.List;

import static com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

@Service
@RequiredArgsConstructor
public class SqsBackoffService {
    private final SqsClient sqsClient;

    @Value("${sqs.backoff.intervals}")
    private final List<Integer> backoffIntervals;
    @Value("${sqs.max-receive-count}")
    private final int maxReceiveCount;
    @Value("${sqs.queue-url}")
    private final String queueUrl;

    public Retry backoff(SQSMessage sqsMessage) {
        int attempt = extractAttempt(sqsMessage);
        var retry = Retry.builder().retried(false)
                .attempt(attempt);

        if (attempt < maxReceiveCount) {
            int delay = computeDelaySeconds(attempt);
            changeVisibility(sqsMessage.getReceiptHandle(), delay);
            return retry.retried(true)
                    .newVisibilityTimeout(delay)
                    .build();
        }
        return retry.build();
    }

    private int extractAttempt(SQSMessage msg) {
        String recvCount = msg.getAttributes().get("ApproximateReceiveCount");
        try {
            return Integer.parseInt(recvCount);
        } catch (Exception e) {
            return 1;
        }
    }

    private int computeDelaySeconds(int attempt) {
        int intervalIndex = Math.min(attempt - 1, backoffIntervals.size() - 1);
        return backoffIntervals.get(intervalIndex);
    }

    private void changeVisibility(String receiptHandle, int delaySeconds) {
        try {
            ChangeMessageVisibilityRequest req = ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .visibilityTimeout(delaySeconds)
                    .build();

            var response = sqsClient.changeMessageVisibility(req);
            System.out.println("Updated visibility timeout:" + response);
        } catch (Exception e) {
            System.err.println("Failed to set visibility timeout: " + e.getMessage());
        }
    }

    @Builder
    public record Retry(boolean retried, int attempt, int newVisibilityTimeout) {
    }

    @StandardException
    public static class SqsBackoffException extends Exception {
    }
}
