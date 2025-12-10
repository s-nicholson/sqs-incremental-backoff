import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { Function, Runtime, Code } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { PolicyStatement, Effect } from 'aws-cdk-lib/aws-iam';
import * as path from 'path';

export class SqsBackoffStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const maxReceiveCount = 5;

    const dlq = new Queue(this, 'DLQ', {
      queueName: 'backoff-dlq.fifo',
      fifo: true,
      visibilityTimeout: Duration.seconds(10)
    });

    const queue = new Queue(this, 'BackoffQueue', {
      queueName: 'backoff.fifo',
      fifo: true,
      visibilityTimeout: Duration.minutes(5),
      retentionPeriod: Duration.days(4),
      contentBasedDeduplication: true,
      deadLetterQueue: {
        queue: dlq,
        maxReceiveCount: maxReceiveCount
      }
    });

    const jarPath = path.join(__dirname, '../..', 'target', 'sqs-backoff-0.1.0-aws.jar'); // adjust version/path

    const fn = new Function(this, 'SqsBackoffHandler', {
      runtime: Runtime.JAVA_21,
      handler: 'org.springframework.cloud.function.adapter.aws.FunctionInvoker::handleRequest', // Spring Cloud Function adapter
      code: Code.fromAsset(jarPath),
      memorySize: 512,
      timeout: Duration.minutes(5),
      environment: {
        QUEUE_URL: queue.queueUrl,
        MAX_RECEIVE: `${maxReceiveCount}`
      }
    });

    queue.grantConsumeMessages(fn);

    const eventSource = new SqsEventSource(queue, {
      batchSize: 10,
      enabled: true,
      reportBatchItemFailures: true,
    });

    fn.addEventSource(eventSource);
  }
}
