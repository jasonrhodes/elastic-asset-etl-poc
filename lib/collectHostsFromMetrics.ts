import { Client } from "@elastic/elasticsearch";
import { getMetricsIndices } from "../constants";
import { SimpleAsset, HostType } from "../types";

interface CollectHosts {
  hosts: SimpleAsset<HostType>[];
}

export async function collectHosts({ esClient }: { esClient: Client }): Promise<SimpleAsset<HostType>[]> {
  const dsl = {
    index: [getMetricsIndices()],
    size: 1000,
    collapse: {
      field: 'host.hostname'
    },
    sort: [
      {
        "@timestamp": "desc"
      }
    ],
    _source: false,
    fields: [
      'kubernetes.*',
      'cloud.*',
      'orchestrator.cluster.name',
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
              field: 'host.hostname'
            }
          },
        ]
      }
    }
  };

  console.log(JSON.stringify(dsl));
  const esResponse = await esClient.search(dsl);

  const assets = esResponse.hits.hits.reduce<CollectHosts>((acc, hit) => {
    const { fields = {} } = hit;
    const hostName = fields['host.hostname'];
    const hostType: HostType = getHostType(fields);

    const host: SimpleAsset<HostType> = {
      '@timestamp': new Date(),
      'asset.type': hostType,
      'asset.kind': 'host',
      'asset.id': hostName,
      'asset.name': hostName,
      'asset.ean': `${hostType}:${hostName}`,
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

    acc.hosts.push(host);

    return acc;
  }, { hosts: [] });

  return assets.hosts;
}

function getHostType(fields: any): HostType {
  if (fields['cloud.provider'] && fields['cloud.service.name']) {
    return `${fields['cloud.provider']}.${fields['cloud.service.name']}`.toLowerCase() as HostType;
  }

  return 'host';
};
