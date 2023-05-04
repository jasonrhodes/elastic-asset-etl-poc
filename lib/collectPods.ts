import { Client } from "@elastic/elasticsearch";
import { getApmIndices, getLogsIndices, getMetricsIndices } from "../constants";
import { SimpleAsset } from "../types";

interface CollectPodsAndNodes {
  pods: SimpleAsset<'k8s.pod'>[];
}

export async function collectPods({ esClient }: { esClient: Client }) {
  const dsl = {
    index: [getLogsIndices(), getApmIndices(), getMetricsIndices()],
    size: 1000,
    collapse: {
      field: 'kubernetes.pod.uid'
    },
    sort: [
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
          { exists: { field: 'kubernetes.pod.uid' } },
          { exists: { field: 'kubernetes.node.name' } }
        ]
      }
    }
  };

  const esResponse = await esClient.search(dsl);

  const docs = esResponse.hits.hits.reduce<CollectPodsAndNodes>((acc, hit) => {
    const { fields = {} } = hit;
    const podUid = fields['kubernetes.pod.uid'];
    const nodeName = fields['kubernetes.node.name'];
    const clusterName = fields['orchestrator.cluster.name'];

    const pod: SimpleAsset<'k8s.pod'> = {
      '@timestamp': new Date(),
      'asset.type': 'k8s.pod',
      'asset.kind': 'pod',
      'asset.id': podUid,
      'asset.ean': `k8s.pod:${podUid}`,
      'asset.parents': [`k8s.node:${nodeName}`]
    };

    if (fields['cloud.provider']) {
      pod['cloud.provider'] = fields['cloud.provider'];
    }

    if (clusterName) {
      pod['orchestrator.cluster.name'] = clusterName;
    }

    acc.pods.push(pod);

    return acc;
  }, { pods: [] });

  return docs.pods;
}
