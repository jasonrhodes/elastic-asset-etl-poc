import yargs from "yargs/yargs";
import { collectHosts } from "./lib/collectHostsFromMetrics";
import { AssetClient, getEsClient } from "./lib/es_client";
import { HostType, SimpleAsset } from "./types";
import config from "./config/config.json";

main();

async function etl(esClient: AssetClient) {
  const hosts = await collectHosts({ esClient: esClient.reader });

  const bulkBody = hosts.flatMap((asset: SimpleAsset<HostType>) => {
    return [
      { create: { _index: `assets-${asset['asset.type']}-default` } },
      asset,
    ];
  })

  const response = await esClient.writer.bulk({ body: bulkBody });

  console.log(`wrote ${response.items.length} assets; errors ? ${response.errors}`);
}

async function main() {
  const argv = yargs(process.argv.slice(2)).options({
    read: { type: 'string', required: true, description: "Name of the cluster in config.json used for reading signals from ES" },
    write: { type: 'string', description: "Name of the cluster in config.json used for writing assets to ES (defaults to the same as read)" }
  }).parseSync();

  if (!(argv.read in config.clusters)) {
    throw new Error("Invalid config values for --read, must be a valid key from config.json 'clusters' map");
  }

  if (argv.write && !(argv.write in config.clusters)) {
    throw new Error("Invalid config values for --write, must be a valid key from config.json 'clusters' map");
  }

  const readConfig = config.clusters[argv.read as keyof typeof config.clusters];
  const writeConfig = config.clusters[(argv.write || argv.read) as keyof typeof config.clusters];
  
  const esClient = await getEsClient({ readConfig, writeConfig });

  await etl(esClient);
  
  console.log('Finished running ETL');
}

