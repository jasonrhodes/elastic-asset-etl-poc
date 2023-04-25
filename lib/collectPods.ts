import { Client } from "@elastic/elasticsearch";
import { getApmIndices, getLogsIndices } from "../constants";
import { SimpleAsset } from "../types";

interface CollectPodsAndNodes {
  pods: SimpleAsset<'k8s.pod'>[];
  nodes: SimpleAsset<'k8s.node'>[];
}

export async function collectPods({ esClient }: { esClient: Client }) {
  // STEP ONE: Query pods that reference their k8s nodes
  const dsl = {
    index: [getLogsIndices(), getApmIndices()],
    size: 1000,
    collapse: {
      field: 'kubernetes.pod.uid'
    },
    sort: [
      {
        "@timestamp": "desc" // TODO: Switch to ASC with a hard-coded "one hour ago" value, then use "search_after" to process all results?
      }
    ],
    _source: false,
    fields: [
      'kubernetes.pod.uid',
      'kubneretes.pod.name',
      'kubernetes.node.id',
      'kubernetes.node.name',
      'kubernetes.namespace',
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
          {
            exists: {
              field: 'kubernetes.pod.uid'
            }
          },
          {
            exists: {
              field: 'kubernetes.node.name'
            }
          }
        ]
      }
    }
  };

  console.log(JSON.stringify(dsl));
  const esResponse = await esClient.search(dsl);

  // STEP TWO: Loop over collected pod documents and create a pod asset doc AND a node asset doc for each
  const docs = esResponse.hits.hits.reduce<CollectPodsAndNodes>((acc, hit) => {
    const { fields = {} } = hit;
    const podUid = fields['kubernetes.pod.uid'];
    const nodeName = fields['kubernetes.node.name'];
    const clusterName = fields['orchestrator.cluster.name'];

    const pod: SimpleAsset<'k8s.pod'> = {
      '@timestamp': new Date(),
      'asset.type': 'k8s.pod',
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

    const foundNode = acc.nodes.find((collectedNode) => collectedNode['asset.ean'] === `k8s.node:${nodeName}`);

    if (foundNode) {
      if (foundNode['asset.children']) {
        foundNode['asset.children'].push(`k8s.pod:${podUid}`);
      } else {
        foundNode['asset.children'] = [`k8s.pod:${podUid}`];
      }
    } else {
      const node: SimpleAsset<'k8s.node'> = {
        '@timestamp': new Date(),
        'asset.type': 'k8s.node',
        'asset.id': nodeName,
        'asset.ean': `k8s.node:${nodeName}`,
        'asset.children': [`k8s.pod:${podUid}`]
      };

      if (clusterName) {
        node['asset.parents'] = [`k8s.cluster:${clusterName}`];
      }

      acc.nodes.push(node);
    }

    return acc;
  }, { pods: [], nodes: [] });

  return { esResponse, docs };
}