'use strict';

const mysql = require('mysql2/promise');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

const secretsClient = new SecretsManagerClient({});
let cachedSecret;
let primaryPool;
let readPool;

async function getDatabaseSecret() {
  if (cachedSecret) return cachedSecret;

  const result = await secretsClient.send(
    new GetSecretValueCommand({ SecretId: process.env.DB_SECRET_ARN }),
  );

  if (!result.SecretString) {
    throw new Error('Database secret did not contain SecretString.');
  }

  cachedSecret = JSON.parse(result.SecretString);
  return cachedSecret;
}

async function createPool(host) {
  const secret = await getDatabaseSecret();
  return mysql.createPool({
    host,
    port: Number(process.env.DB_PORT || 3306),
    database: process.env.DB_NAME,
    user: secret.username,
    password: secret.password,
    waitForConnections: true,
    connectionLimit: 3,
    queueLimit: 10,
    enableKeepAlive: true,
    ssl: { rejectUnauthorized: true },
  });
}

async function getPrimaryPool() {
  if (!primaryPool) primaryPool = await createPool(process.env.DB_HOST);
  return primaryPool;
}

async function getReadPool() {
  if (!readPool) {
    readPool = await createPool(
      process.env.DB_READ_HOST || process.env.DB_HOST,
    );
  }
  return readPool;
}

function monthPeriod() {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 0));
  return {
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
  };
}

async function upsertMetric(
  connection,
  companyId,
  metricName,
  metricValue,
  periodStart,
  periodEnd,
  metadata = {},
) {
  await connection.execute(
    `INSERT INTO analytics_summaries
       (company_id, metric_name, metric_value, period_start, period_end, metadata_json)
     VALUES (?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       metric_value = VALUES(metric_value),
       metadata_json = VALUES(metadata_json),
       updated_at = UTC_TIMESTAMP()`,
    [
      companyId,
      metricName,
      metricValue,
      periodStart,
      periodEnd,
      JSON.stringify(metadata),
    ],
  );
}

exports.handler = async () => {
  const readDb = await getReadPool();
  const writeDb = await getPrimaryPool();
  const period = monthPeriod();

  const [reviewRows] = await readDb.execute(
    `SELECT
       company_id,
       COUNT(*) AS review_count,
       ROUND(AVG(rating), 2) AS average_rating
     FROM reviews
     WHERE status = 'APPROVED'
       AND created_at >= ?
       AND created_at < DATE_ADD(?, INTERVAL 1 DAY)
     GROUP BY company_id`,
    [period.start, period.end],
  );

  const [wellbeingRows] = await readDb.execute(
    `SELECT
       company_id,
       COUNT(*) AS checkin_count,
       ROUND(AVG(workload_score), 2) AS average_workload,
       ROUND(AVG(stress_score), 2) AS average_stress,
       ROUND(AVG(support_score), 2) AS average_support
     FROM wellbeing_checkins
     WHERE created_at >= ?
       AND created_at < DATE_ADD(?, INTERVAL 1 DAY)
     GROUP BY company_id`,
    [period.start, period.end],
  );

  const connection = await writeDb.getConnection();

  try {
    await connection.beginTransaction();

    for (const row of reviewRows) {
      await upsertMetric(
        connection,
        row.company_id,
        'review_count',
        Number(row.review_count),
        period.start,
        period.end,
      );
      await upsertMetric(
        connection,
        row.company_id,
        'average_rating',
        Number(row.average_rating || 0),
        period.start,
        period.end,
      );
    }

    for (const row of wellbeingRows) {
      const workload = Number(row.average_workload || 0);
      const stress = Number(row.average_stress || 0);
      const support = Number(row.average_support || 0);
      const burnoutIndicator = Math.max(
        0,
        Math.min(5, Number(((workload + stress + (6 - support)) / 3).toFixed(2))),
      );

      await upsertMetric(
        connection,
        row.company_id,
        'wellbeing_checkin_count',
        Number(row.checkin_count),
        period.start,
        period.end,
      );
      await upsertMetric(
        connection,
        row.company_id,
        'average_workload_score',
        workload,
        period.start,
        period.end,
      );
      await upsertMetric(
        connection,
        row.company_id,
        'average_stress_score',
        stress,
        period.start,
        period.end,
      );
      await upsertMetric(
        connection,
        row.company_id,
        'average_support_score',
        support,
        period.start,
        period.end,
      );
      await upsertMetric(
        connection,
        row.company_id,
        'burnout_indicator',
        burnoutIndicator,
        period.start,
        period.end,
        {
          method: 'PoC composite of workload, stress, and inverse support',
          scale: '0-5',
        },
      );
    }

    await connection.commit();

    return {
      statusCode: 200,
      period,
      companiesWithReviews: reviewRows.length,
      companiesWithCheckins: wellbeingRows.length,
    };
  } catch (error) {
    await connection.rollback();
    console.error('Analytics calculation failed', {
      name: error.name,
      message: error.message,
    });
    throw error;
  } finally {
    connection.release();
  }
};
