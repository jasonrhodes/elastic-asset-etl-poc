export interface SimpleAsset<T> {
  '@timestamp': Date;
  'asset.ean': string;
  'asset.type': T;
  'asset.id': string;
  'asset.kind': string;
  'asset.name'?: string;
  'asset.parents'?: string[];
  'asset.children'?: string[];
  'asset.references'?: string[];
  'cloud.provider'?: string;
  'cloud.service.name'?: string;
  'cloud.region'?: string;
  'cloud.instance.id'?: string;
  'orchestrator.cluster.name'?: string;
  'service.environment'?: string;
  'kubernetes.node.hostname'?: string;
}

export type HostType = 'host' | 'aws.ec2' | 'k8s.node' | 'gcp.gce';
