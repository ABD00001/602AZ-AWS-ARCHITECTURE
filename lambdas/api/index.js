'use strict';

const mysql = require('mysql2/promise');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const secretsClient = new SecretsManagerClient({});
const sqsClient = new SQSClient({});

let cachedSecret;
let databasePool;

function buildResponse(statusCode, body) {
  return {
    statusCode,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': process.env.ALLOWED_ORIGIN,
      'Access-Control-Allow-Headers': 'Content-Type,Authorization',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    },
    body: JSON.stringify(body),
  };
}

async function getDatabaseSecret() {
  if (cachedSecret) {
    return cachedSecret;
  }

  const result = await secretsClient.send(
    new GetSecretValueCommand({ SecretId: process.env.DB_SECRET_ARN }),
  );

  if (!result.SecretString) {
    throw new Error('The database secret did not contain a SecretString value.');
  }

  cachedSecret = JSON.parse(result.SecretString);
  return cachedSecret;
}

async function getDatabasePool() {
  if (databasePool) {
    return databasePool;
  }

  const secret = await getDatabaseSecret();

  databasePool = mysql.createPool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT || 3306),
    database: process.env.DB_NAME,
    user: secret.username,
    password: secret.password,
    waitForConnections: true,
    connectionLimit: 2,
    queueLimit: 0,
    enableKeepAlive: true,
  });

  return databasePool;
}

function getClaims(event) {
  return event.requestContext?.authorizer?.claims || {};
}

async function checkDatabase() {
  const pool = await getDatabasePool();
  const [rows] = await pool.query(
    'SELECT 1 AS connection_ok, DATABASE() AS database_name, NOW() AS database_time',
  );

  return rows[0];
}

async function sendQueueTest(event) {
  let requestBody = {};

  if (event.body) {
    try {
      requestBody = JSON.parse(event.body);
    } catch {
      return buildResponse(400, { message: 'The request body must be valid JSON.' });
    }
  }

  const claims = getClaims(event);
  const job = {
    jobId: `demo-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    type: 'ARCHITECTURE_DEMO',
    createdAt: new Date().toISOString(),
    requestedBy: claims.sub || 'authenticated-user',
    userGroups: claims['cognito:groups'] || '',
    message: requestBody.message || 'FairWork Pulse SQS integration test',
    forceFailure: requestBody.forceFailure === true,
  };

  const result = await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: process.env.JOB_QUEUE_URL,
      MessageBody: JSON.stringify(job),
    }),
  );

  return buildResponse(202, {
    message: 'The demonstration message was accepted by SQS.',
    jobId: job.jobId,
    sqsMessageId: result.MessageId,
    forceFailure: job.forceFailure,
  });
}

exports.handler = async (event) => {
  const method = event.httpMethod;
  const path = event.resource || event.path;

  console.log('API request received', {
    method,
    path,
    requestId: event.requestContext?.requestId,
  });

  try {
    if (method === 'GET' && path === '/health') {
      return buildResponse(200, {
        status: 'ok',
        service: 'fairwork-pulse-602az-api',
        timestamp: new Date().toISOString(),
      });
    }

    if (method === 'GET' && path === '/database-health') {
      const databaseResult = await checkDatabase();
      return buildResponse(200, {
        status: 'ok',
        connection: 'API Lambda to private RDS primary',
        database: databaseResult,
      });
    }

    if (method === 'POST' && path === '/queue-test') {
      return await sendQueueTest(event);
    }

    return buildResponse(404, {
      message: 'Route not found in the 602AZ proof of concept.',
    });
  } catch (error) {
    console.error('API request failed', {
      name: error.name,
      message: error.message,
    });

    return buildResponse(500, {
      message: 'The architecture demonstration request failed.',
      errorType: error.name,
    });
  }
};
