import { Client } from "@elastic/elasticsearch";
import { getApmIndices, getLogsIndices, getMetricsIndices } from "../constants";
import { SimpleAsset } from "../types";

interface CollectContainers {
  containers: SimpleAsset<'container'>[];
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
        must: [
          { exists: { field: 'kubernetes.container.id' } },
          { exists: { field: 'kubernetes.pod.uid' } },
          { exists: { field: 'host.hostname' } },
        ]
      }
    }
  };

  const esResponse = await esClient.search(dsl);

  // STEP TWO: Loop over collected pod documents and create a pod asset doc AND a node asset doc for each
  const docs = esResponse.hits.hits.reduce<CollectContainers>((acc, hit) => {
    const { fields = {} } = hit;
    const containerId = fields['container.id'];
    const podUid = fields['kubernetes.pod.uid'];
    const nodeName = fields['kubernetes.node.name'];

    const parentEan = podUid ? `k8s.pod:${podUid}` : fields['host.hostname'];

    const container: SimpleAsset<'container'> = {
      '@timestamp': new Date(),
      'asset.type': 'container',
      'asset.kind': 'container',
      'asset.id': containerId,
      'asset.ean': `container:${containerId}`,
      'asset.parents': parentEan && [parentEan],
    };

    acc.containers.push(container);

    return acc;
  }, { containers: [] });

  return docs.containers;
}

