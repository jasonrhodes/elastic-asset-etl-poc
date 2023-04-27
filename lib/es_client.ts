import { Client, ClientOptions } from '@elastic/elasticsearch';
import { assetsIndexTemplateConfig } from './assets_index_template';
import { BulkRequest, IndicesPutIndexTemplateRequest, SearchRequest } from '@elastic/elasticsearch/lib/api/types';

let singletonClient: AssetClient | null = null;

interface AssetClientOptions {
  readConfig: ClientOptions;
  writeConfig?: ClientOptions;
}

export class AssetClient {
  public reader: Client;
  public writer: Client;

  constructor({ readConfig, writeConfig = readConfig }: AssetClientOptions) {
    this.reader = new Client(readConfig);
    this.writer = new Client(writeConfig);
  }

  public async putIndexTemplate(req: IndicesPutIndexTemplateRequest) {
    return await this.writer.indices.putIndexTemplate(req);
  }

  public async search(req: SearchRequest) {
    return await this.reader.search(req);
  }

  public async bulk<T>(req: BulkRequest<T>) {
    return await this.writer.bulk<T>(req);
  }
}

export async function getEsClient(options: AssetClientOptions) {
  if (singletonClient) {
    return singletonClient;
  }

  singletonClient = new AssetClient(options);
  // await template creation?

  await singletonClient.putIndexTemplate(assetsIndexTemplateConfig);
  return singletonClient;
}