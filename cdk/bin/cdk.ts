#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { SqsBackoffStack } from '../lib/sqs-backoff-stack';

const app = new cdk.App();
new SqsBackoffStack(app, 'SqsBackoffStack');