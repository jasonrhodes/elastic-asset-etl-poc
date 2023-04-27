export const LOGS_INDICES = 'logs-*,filebeat-*';
export const APM_INDICES = 'traces-*,apm*,metrics-apm*';
export const METRICS_INDICES = 'metrics-*,metricbeat-*';

export const REMOTE_LOGS_INDICES = 'remote_cluster:logs-*,remote_cluster:filebeat-*';
export const REMOTE_APM_INDICES = 'remote_cluster:traces-*,remote_cluster:apm*,remote_cluster:metrics-apm*';
export const REMOTE_METRICS_INDICES = 'remote_cluster:metrics-*,remote_cluster:metricbeat-*';

export function getLogsIndices() {
  if (process.env.ES_IS_CCS === "true") {
    return REMOTE_LOGS_INDICES;
  } else {
    return LOGS_INDICES;
  }
}

export function getApmIndices() {
  if (process.env.ES_IS_CCS === "true") {
    return REMOTE_APM_INDICES;
  } else {
    return APM_INDICES;
  }
}

export function getMetricsIndices() {
  if (process.env.ES_IS_CCS === "true") {
    return REMOTE_METRICS_INDICES;
  } else {
    return METRICS_INDICES;
  }
}
