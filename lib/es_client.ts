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

  if (!process.env.ES_USERNAME || !process.env.ES_PASSWORD) {
    throw new Error('Please provide username and password for Elasticsearch via ES_USERNAME and ES_PASSWORD env vars');
  }

  const tlsRejectUnauthorized = (process.env.ASSETS_READ_ES_TLS_REJECT_UNAUTHORIZED === "false") ? false : true;

  singletonClient = new AssetClient(options);
  // await template creation?

  await singletonClient.putIndexTemplate(assetsIndexTemplateConfig);
  return singletonClient;
}