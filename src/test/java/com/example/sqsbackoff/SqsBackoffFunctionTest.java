package com.example.sqsbackoff;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.test.FunctionalSpringBootTest;

import java.util.List;
import java.util.function.Function;

import static com.example.sqsbackoff.SqsBackoffService.Retry;
import static com.example.sqsbackoff.SqsBackoffService.SqsBackoffException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@FunctionalSpringBootTest
class SqsBackoffFunctionTest {
    @Autowired
    private FunctionCatalog catalog;

    @MockBean
    private MessageProcessor messageProcessor;
    @MockBean
    private SqsBackoffService sqsBackoffService;

    @Test
    @SneakyThrows
    public void successful_message_is_dismissed() {
        Function<SQSEvent, SQSBatchResponse> handler = catalog.lookup(Function.class,
                "sqsHandler");

        var msg = message();
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(msg));

        doNothing().when(messageProcessor).processBusiness(any());

        SQSBatchResponse response = handler.apply(event);

        assertThat(response.getBatchItemFailures()).isEmpty();
        verifyNoInteractions(sqsBackoffService);
    }

    @Test
    @SneakyThrows
    public void retried_message_goes_back_to_queue() {
        Function<SQSEvent, SQSBatchResponse> handler = catalog.lookup(Function.class,
                "sqsHandler");

        var msg = message();
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(msg));

        doThrow(SqsBackoffException.class)
                .when(messageProcessor).processBusiness(any());
        when(sqsBackoffService.backoff(msg))
                .thenReturn(Retry.builder()
                        .retried(true)
                        .newVisibilityTimeout(30)
                        .build());

        SQSBatchResponse response = handler.apply(event);

        assertThat(response.getBatchItemFailures()).hasSize(1);
        assertThat(response.getBatchItemFailures().get(0).getItemIdentifier())
                .isEqualTo("123");
    }

    @Test
    @SneakyThrows
    public void message_with_no_attempts_remaining_is_dismissed() {
        Function<SQSEvent, SQSBatchResponse> handler = catalog.lookup(Function.class,
                "sqsHandler");

        var msg = message();
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(msg));

        doThrow(SqsBackoffException.class)
                .when(messageProcessor).processBusiness(any());
        when(sqsBackoffService.backoff(msg))
                .thenReturn(Retry.builder()
                        .retried(false)
                        .build());

        SQSBatchResponse response = handler.apply(event);

        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    @SneakyThrows
    public void non_backoff_exception_goes_back_to_queue() {
        Function<SQSEvent, SQSBatchResponse> handler = catalog.lookup(Function.class,
                "sqsHandler");

        var msg = message();
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(msg));

        doThrow(RuntimeException.class)
                .when(messageProcessor).processBusiness(any());

        SQSBatchResponse response = handler.apply(event);

        assertThat(response.getBatchItemFailures()).hasSize(1);
        assertThat(response.getBatchItemFailures().get(0).getItemIdentifier())
                .isEqualTo("123");
        verifyNoInteractions(sqsBackoffService);
    }

    private static SQSEvent.SQSMessage message() {
        SQSEvent.SQSMessage msg = new SQSEvent.SQSMessage();
        msg.setMessageId("123");
        msg.setReceiptHandle("abc");
        msg.setBody("""
        {
            "payload": {
                "test": "something"
            }
        }
        """);
        msg.setAttributes(
                java.util.Map.of("ApproximateReceiveCount", "1")
        );
        return msg;
    }
}