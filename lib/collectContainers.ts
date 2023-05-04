import { Client } from "@elastic/elasticsearch";
import { getApmIndices, getLogsIndices, getMetricsIndices } from "../constants";
import { SimpleAsset } from "../types";

interface CollectContainers {
  containers: SimpleAsset[];
}

export async function collectContainers({ esClient }: { esClient: Client }) {
  const dsl = {
    index: [getLogsIndices(), getApmIndices(), getMetricsIndices()],
    size: 1000,
    collapse: {
      field: 'container.id'
    },
    sort: [
      { '_score': 'desc' },
      { '@timestamp': 'desc' }
    ],
    _source: false,
    fields: [
      'kubernetes.*',
      'cloud.provider',
      'orchestrator.cluster.name',
      'host.name',
      'host.hostname'
    ],
    query: {
      bool: {
        filter: [
          {
            range: {
              '@timestamp': {
                gte: 'now-1h'
              }
            }
          }
        ],
        should: [
          { exists: { field: 'kubernetes.container.id' } },
          { exists: { field: 'kubernetes.pod.uid' } },
          { exists: { field: 'host.hostname' } },
        ]
      }
    }
  };

  const esResponse = await esClient.search(dsl);

  const docs = esResponse.hits.hits.reduce<CollectContainers>((acc, hit) => {
    const { fields = {} } = hit;
    const containerId = fields['container.id'];
    const podUid = fields['kubernetes.pod.uid'];
    const nodeName = fields['kubernetes.node.name'];

    const parentEan = podUid ? `pod:${podUid}` : `host:${fields['host.hostname']}`;

    const container: SimpleAsset = {
      '@timestamp': new Date(),
      'asset.kind': 'container',
      'asset.id': containerId,
      'asset.ean': `container:${containerId}`,
      'asset.parents': [parentEan],
    };

    acc.containers.push(container);

    return acc;
  }, { containers: [] });

  return docs.containers;
}

