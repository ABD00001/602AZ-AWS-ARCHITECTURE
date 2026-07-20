'use strict';

const mysql = require('mysql2/promise');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

const secretsClient = new SecretsManagerClient({});

async function getSecret() {
  const result = await secretsClient.send(
    new GetSecretValueCommand({ SecretId: process.env.DB_SECRET_ARN }),
  );

  if (!result.SecretString) {
    throw new Error('The database secret did not contain a SecretString value.');
  }

  return JSON.parse(result.SecretString);
}

exports.handler = async () => {
  const secret = await getSecret();
  const connection = await mysql.createConnection({
    host: process.env.DB_READ_HOST,
    port: Number(process.env.DB_PORT || 3306),
    database: process.env.DB_NAME,
    user: secret.username,
    password: secret.password,
    connectTimeout: 10000,
  });

  try {
    const [rows] = await connection.query(
      'SELECT 1 AS connection_ok, DATABASE() AS database_name, NOW() AS replica_time',
    );

    const result = rows[0];

    console.log('Scheduled read-replica check completed', {
      connection: 'EventBridge to Analytics Lambda to RDS read replica',
      databaseName: result.database_name,
      replicaTime: result.replica_time,
    });

    return {
      statusCode: 200,
      connection: 'RDS read replica',
      result,
    };
  } catch (error) {
    console.error('Scheduled read-replica check failed', {
      name: error.name,
      message: error.message,
    });
    throw error;
  } finally {
    await connection.end();
  }
};
