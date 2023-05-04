import { Client } from "@elastic/elasticsearch";
import { getApmIndices, getLogsIndices, getMetricsIndices } from "../constants";
import { SimpleAsset, HostType } from "../types";

interface CollectHosts {
  hosts: SimpleAsset<HostType>[];
}

export async function collectHosts({ esClient }: { esClient: Client }): Promise<SimpleAsset<HostType>[]> {
  const dsl = {
    index: [getMetricsIndices(), getLogsIndices(), getApmIndices()],
    size: 1000,
    collapse: { field: 'host.hostname' },
    sort: [
      { "_score": "desc" },
      { "@timestamp": "desc" }
    ],
    _source: false,
    fields: [
      '@timestamp',
      'cloud.*',
      'container.*',
      'host.hostname',
      'kubernetes.*',
      'orchestrator.cluster.name',
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
          { exists: { field: 'host.hostname' } },
        ],
        should: [
          { exists: { field: 'kubernetes.node.name' } },
          { exists: { field: 'kubernetes.pod.uid' } },
          { exists: { field: 'container.id' } }
        ]
      }
    }
  };

  const esResponse = await esClient.search(dsl);

  const assets = esResponse.hits.hits.reduce<CollectHosts>((acc, hit) => {
    const { fields = {} } = hit;
    const hostName = fields['host.hostname'];
    const k8sNode = fields['kubernetes.node.name'];
    const k8sPod = fields['kubernetes.pod.uid'];

    const hostEan = `${k8sNode ? 'k8s.node:' + k8sNode : 'host:' + hostName}`;

    const host: SimpleAsset<HostType> = {
      '@timestamp': new Date(),
      'asset.type': k8sNode ? 'k8s.node' : 'host',
      'asset.kind': 'host',
      'asset.id': k8sNode || hostName,
      'asset.name': k8sNode || hostName,
      'asset.ean': hostEan,
    };

    if (fields['cloud.provider']) {
      host['cloud.provider'] = fields['cloud.provider'];
    }

    if (fields['cloud.instance.id']) {
      host['cloud.instance.id'] = fields['cloud.instance.id'];
    }

    if (fields['cloud.service.name']) {
      host['cloud.service.name'] = fields['cloud.service.name'];
    }

    if (fields['cloud.region']) {
      host['cloud.region'] = fields['cloud.region'];
    }

    if (fields['orchestrator.cluster.name']) {
      host['orchestrator.cluster.name'] = fields['orchestrator.cluster.name'];
    }

    if (k8sPod) {
      host['asset.children'] = [`k8s.pod:${k8sPod}`];
    }

    acc.hosts.push(host);

    return acc;
  }, { hosts: [] });

  return assets.hosts;
}
