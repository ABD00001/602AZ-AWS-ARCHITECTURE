'use strict';

exports.handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records || []) {
    try {
      const message = JSON.parse(record.body);

      console.log('SQS demonstration message received', {
        messageId: record.messageId,
        jobId: message.jobId,
        type: message.type,
        createdAt: message.createdAt,
        project: process.env.PROJECT_NAME,
      });

      // This controlled option makes it possible to demonstrate retries and the DLQ.
      if (message.forceFailure === true) {
        throw new Error('Controlled demonstration failure requested.');
      }

      console.log('SQS demonstration message processed successfully', {
        messageId: record.messageId,
        jobId: message.jobId,
      });
    } catch (error) {
      console.error('SQS message processing failed', {
        messageId: record.messageId,
        name: error.name,
        message: error.message,
      });

      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  return { batchItemFailures };
};
