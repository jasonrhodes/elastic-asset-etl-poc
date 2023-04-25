export interface SimpleAsset<T> {
  '@timestamp': Date;
  'asset.ean': string;
  'asset.type': T;
  'asset.id': string;
  'asset.name'?: string;
  'asset.parents'?: string[];
  'asset.children'?: string[];
  'asset.references'?: string[];
  'cloud.provider'?: string;
  'orchestrator.cluster.name'?: string;
  'service.environment'?: string;
}