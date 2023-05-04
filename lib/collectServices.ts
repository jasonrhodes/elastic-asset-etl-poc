import { Client } from "@elastic/elasticsearch";
import { getApmIndices, getLogsIndices, getMetricsIndices } from "../constants";
import { SimpleAsset } from "../types";

interface CollectServices {
  services: SimpleAsset[];
}

const MISSING_KEY = "__unknown__";

export async function collectServices({ esClient }: { esClient: Client }): Promise<SimpleAsset[]> {
  const dsl = {
    index: getApmIndices(),
    "size": 0,
    "sort": [
      {
        "@timestamp": "desc"
      }
    ],
    "_source": false,
    "query": {
      "bool": {
        "filter": [
          {
            "range": {
              "@timestamp": {
                "gte": "now-1h"
              }
            }
          }
        ],
        "must": [
          {
            "exists": {
              "field": "service.name"
            }
          }
        ]
      }
    },
    "aggs": {
      "service_environment": {
        "multi_terms": {
          "size": 100,
          "terms": [
            {
              "field": "service.name"
            },
            {
              "field": "service.environment",
              "missing": MISSING_KEY
            }
          ]
        },
        "aggs": {
          "container_host": {
            "multi_terms": {
              "size": 100,
              "terms": [
                { "field": "container.id", "missing": MISSING_KEY },
                { "field": "host.hostname", "missing": MISSING_KEY }
              ]
            }
          }
        }
      }
    }
  };


  const esResponse = await esClient.search(dsl);
  const serviceEnvironment = esResponse.aggregations?.service_environment as { buckets: any[] };

  const docs = serviceEnvironment.buckets.reduce<CollectServices>((acc: any, hit: any) => {
    const [serviceName, environment] = hit.key;
    const containerHosts = hit.container_host.buckets;

    const service: SimpleAsset = {
      '@timestamp': new Date(),
      'asset.kind': 'service',
      'asset.id': serviceName,
      'asset.ean': `service:${serviceName}`,
      'asset.references': [],
      'asset.parents': [],
    };

    if (environment != MISSING_KEY) {
      service['service.environment'] = environment;
    }

    containerHosts.forEach((hit: any) => {
      const [containerId, hostname] = hit.key;
      if (containerId !== MISSING_KEY) {
        service['asset.parents']?.push(`container:${containerId}`);
      }

      if (hostname !== MISSING_KEY) {
        service['asset.references']?.push(`host:${hostname}`);
      }
    });

    acc.services.push(service);

    return acc;
  }, { services: [] });

  return [...docs.services];
}
