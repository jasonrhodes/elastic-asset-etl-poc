import { Client } from "@elastic/elasticsearch";
import { getApmIndices, getLogsIndices } from "../constants";
import { SimpleAsset } from "../types";

interface CollectServices {
  services: SimpleAsset<'service'>[];
  containers: SimpleAsset<'container'>[];
}

export async function collectServices({ esClient }: { esClient: Client }) {
  // STEP ONE: Query pods that reference their k8s nodes
  const dsl = {
    index: [getApmIndices()],
    size: 1000,
    collapse: {
      field: 'service.name'
    },
    sort: [
      {
        "@timestamp": "desc" // TODO: Switch to ASC with a hard-coded "one hour ago" value, then use "search_after" to process all results?
      }
    ],
    _source: false,
    fields: [
      'service.name',
      'service.environment',
      'container.*',
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
              field: 'service.name'
            }
          }
        ],
        should: [
          {
            exists: {
              field: 'container.id'
            }
          },
          {
            exists: {
              field: 'kubernetes.pod.uid'
            }
          },
          {
            exists: {
              field: 'host.name'
            }
          },
          {
            exists: {
              field: 'host.hostname'
            }
          }
        ],
        minimum_should_match: 1
      }
    }
  };

  console.log(JSON.stringify(dsl));
  const esResponse = await esClient.search(dsl);

  const docs = esResponse.hits.hits.reduce<CollectServices>((acc, hit) => {
    const { fields = {} } = hit;
    const serviceName = fields['service.name'];
    const serviceEnvironment = fields['service.environment'];
    const containerId = fields['container.id'];
    const podUid = fields['kubernetes.pod.uid'];
    const nodeName = fields['kubernetes.node.name'];

    const serviceEan = `service:${serviceName}`;
    const containerEan = containerId ? `container:${containerId}` : null;
    const podEan = podUid ? `k8s.pod:${podUid}` : null;
    const nodeEan = nodeName ? `k8s.node:${nodeName}` : null;
    const service: SimpleAsset<'service'> = {
      '@timestamp': new Date(),
      'asset.type': 'service',
      'asset.id': serviceName,
      'asset.ean': serviceEan,
      'asset.references': [],
      'service.environment': serviceEnvironment // TODO: Should this be part of the service's ID/EAN?
    };

    if (containerEan) {
      service['asset.parents'] = [containerEan];
    }

    if (fields['cloud.provider']) {
      service['cloud.provider'] = fields['cloud.provider'];
    }

    if (podEan) {
      service['asset.references']?.push(podEan);
    }

    if (nodeEan) {
      service['asset.references']?.push(nodeEan);
    }

    acc.services.push(service);

    if (!containerEan) {
      return acc;
    }

    const foundContainer = acc.containers.find((collectedContainer) => collectedContainer['asset.ean'] === containerEan);

    if (foundContainer) {
      if (foundContainer['asset.children']) {
        foundContainer['asset.children'].push(serviceEan);
      } else {
        foundContainer['asset.children'] = [serviceEan];
      }

      if (podEan) {
        if (foundContainer['asset.parents']) {
          foundContainer['asset.parents'].push(podEan);
        } else {
          foundContainer['asset.parents'] = [podEan];
        }
      }

      if (nodeEan) {
        foundContainer['asset.references']?.push(nodeEan);
      }
    } else {
      const container: SimpleAsset<'container'> = {
        '@timestamp': new Date(),
        'asset.type': 'container',
        'asset.id': containerId,
        'asset.ean': containerEan,
        'asset.references': [],
        'asset.children': [serviceEan]
      };

      if (podEan) {
        container['asset.parents'] = [podEan];
      }

      if (nodeEan) {
        container['asset.references']?.push(nodeEan);
      }

      acc.containers.push(container);
    }

    return acc;
  }, { services: [], containers: [] });

  return { esResponse, docs };
}