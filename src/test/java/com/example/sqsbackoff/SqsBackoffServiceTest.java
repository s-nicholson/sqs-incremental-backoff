package com.example.sqsbackoff;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SqsBackoffServiceTest {
    private static final List<Integer> INTERVALS = List.of(600, 900, 1200);
    private static final String QUEUE_URL = "QUEUE";
    private static final int MAX_RECEIVE_COUNT = 5;

    @Mock
    private SqsClient sqsClient;

    @ParameterizedTest
    @MethodSource("retryResults")
    void retries_up_to_max_retries(int attempt, int newVisibilityTimeout, boolean retried) {
        lenient().when(sqsClient.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenReturn(mock(ChangeMessageVisibilityResponse.class));

        SqsBackoffService sqsBackoffService = new SqsBackoffService(sqsClient, INTERVALS, MAX_RECEIVE_COUNT, QUEUE_URL);
        var message = message(attempt);
        var retry = sqsBackoffService.backoff(message);

        assertEquals(retried, retry.retried());
        assertEquals(newVisibilityTimeout, retry.newVisibilityTimeout());
        if (retried) {
            verify(sqsClient).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
        } else {
            verifyNoInteractions(sqsClient);
        }
    }

    static Stream<Arguments> retryResults() {
        return Stream.of(
                Arguments.of(1, INTERVALS.get(0), true),
                Arguments.of(2, INTERVALS.get(1), true),
                Arguments.of(3, INTERVALS.get(2), true),
                Arguments.of(4, INTERVALS.get(2), true),
                Arguments.of(5, 0, false)
        );
    }

    private static SQSEvent.SQSMessage message(int attempt) {
        var message = new SQSEvent.SQSMessage();
        message.setAttributes(Map.of(
                "ApproximateReceiveCount", String.valueOf(attempt)
        ));
        return message;
    }
}