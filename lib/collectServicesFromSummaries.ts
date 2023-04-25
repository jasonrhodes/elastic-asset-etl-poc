import { getApmIndices, getLogsIndices } from "../constants";
import { SearchRequest, SortResults } from "@elastic/elasticsearch/lib/api/types";
import { AssetClient } from "./es_client";

interface ServiceResult {
  '@timestamp': number;
  name: string;
  environment: string;
}

async function batchSearchForServices({ esClient, searchAfter, resultsAcc = [] }: { esClient: AssetClient, searchAfter?: SortResults; resultsAcc?: ServiceResult[] }): Promise<{ services: ServiceResult[], lastSort?: SortResults }> {
  const dsl: SearchRequest = {
    index: [getApmIndices()],
    size: 1000,
    // collapse: {
    //   field: 'service.name'
    // },
    sort: [
      {
        "@timestamp": "asc"
      }
    ],
    _source: false,
    fields: [
      '@timestamp',
      'service.name',
      'service.environment',
    ],
    query: {
      bool: {
        filter: [
          {
            range: {
              '@timestamp': {
                gte: 'now-10m'
              }
            }
          }
        ],
        must: [
          {
            term: {
              "metricset.name" : "service_summary"
            }
          }
        ],
      }
    }
  };

  if (searchAfter) {
    dsl.search_after = searchAfter;
  }

  console.log(`Searching for batch of service summaries (search_after: ${searchAfter})`);
  const response = await searchAndPrintQueryOnError(esClient, dsl);

  const processed = response.hits.hits.flatMap((result) => {
    if (!result.fields) {
      // Simplest way to remove unwanted elements from an array
      // while in a map function is to use .flatMap and return
      // an empty array for the items you want to remove.
      return [];
    }
    return {
      '@timestamp': result.fields['@timestamp'],
      name: result.fields['service.name'][0],
      environment: result.fields['service.environment'][0]
    }
  });

  try {
    const updatedResults = [...resultsAcc, ...processed];
    const lastHit = response.hits.hits.slice(-1)[0];

    if (
      typeof response.hits.total !== "number"
      && response.hits.total
      && response.hits.total.value > response.hits.hits.length
      && response.hits.hits.length > 0
      && lastHit
    ) {
      return await batchSearchForServices({ esClient, searchAfter: lastHit.sort, resultsAcc: updatedResults });
    }

    const deduped = updatedResults.reduce<ServiceResult[]>((all, next) => {
      const exists = all.find((s) => s.name === next.name && s.environment === next.environment);
      if (exists) {
        exists['@timestamp'] = next['@timestamp'];
      } else {
        all.push(next);
      }
      return all;
    }, []);

    return { services: deduped, lastSort: lastHit && lastHit.sort ? lastHit.sort : searchAfter };

  } catch (error: any) {
    console.log("An error occurred!", error.message);
    console.log('\nQUERY:');
    console.log(JSON.stringify(dsl));
    console.log('\nLAST HIT:');
    const lastHit = response.hits.hits.slice(-1);
    console.log(JSON.stringify(lastHit));
    console.log('\nRESPONSE:');
    console.log(`${response.hits.hits.length} hits returned`);
    response.hits.hits = [];
    console.log(JSON.stringify(response));

    throw error;
  }
}

function singleItem(x: any) {
  if (!x || !Array.isArray(x)) {
    return undefined;
  }
  return x[0];
}

function findParent({ containerId, podUid, hostname, hosthostname }: Partial<ServiceParentResult>) {
  if (containerId) {
    return {
      parentType: 'container',
      parentId: containerId
    };
  }

  if (podUid) {
    return {
      parentType: 'k8s.pod',
      parentId: podUid
    };
  }

  if (hostname) {
    return {
      parentType: 'host.name',
      parentId: hostname
    };
  }

  if (hosthostname) {
    return {
      parentType: 'host.hostname',
      parentId: hosthostname
    };
  }

  return {
    parentType: 'unknown',
    parentId: ''
  };
}

interface ServiceParentResult {
  '@timestamp': number;
  name: string;
  environment: string;
  parentType: string;
  parentId: string;
  containerId?: string;
  podUid?: string;
  hostname?: string;
  hosthostname?: string;
  'event.dataset'?: string;
  'data_stream.dataset'?: string;
}

interface BatchSearchForServiceParentsOptions {
  esClient: AssetClient; 
  searchAfter?: SortResults; 
  resultsAcc?: ServiceParentResult[];
  services: ServiceResult[];
}

async function batchSearchForServiceParents({ esClient, services, searchAfter, resultsAcc = [] }: BatchSearchForServiceParentsOptions): Promise<{ services: ServiceParentResult[], lastSort?: SortResults }> {
  const dsl: SearchRequest = {
    index: [getApmIndices()],
    size: 1000,
    sort: [
      {
        "@timestamp": "asc"
      }
    ],
    _source: false,
    fields: [
      '@timestamp',
      'data_stream.dataset',
      'event.dataset',
      'service.name',
      'service.environment',
      'container.id',
      'kubernetes.pod.uid',
      'kubernetes.pod.name',
      'host.*'
    ],
    query: {
      bool: {
        filter: [
          {
            range: {
              '@timestamp': {
                gte: 'now-15m'
              }
            }
          }
        ],
        must: [
          {
            terms: {
              "service.name": services.map(s => s.name)
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
  }

  if (searchAfter) {
    dsl.search_after = searchAfter;
  }

  console.log(`Searching for batch of service parents (search_after: ${searchAfter})`);
  const response = await searchAndPrintQueryOnError(esClient ,dsl);

  const processed = response.hits.hits.flatMap((result) => {
    if (!result.fields) {
      // Simplest way to remove unwanted elements from an array
      // while in a map function is to use .flatMap and return
      // an empty array for the items you want to remove.
      return [];
    }

    const f = result.fields;

    const containerId = singleItem(f['container.id']);
    const podUid = singleItem(f['kubernetes.pod.uid']);
    const hostname = singleItem(f['host.name']);
    const hosthostname = singleItem(f['host.hostname']);
    return {
      '@timestamp': f['@timestamp'],
      name: singleItem(f['service.name']),
      environment: singleItem(f['service.environment']),
      'event.dataset': singleItem(f['event.dataset']),
      'data_stream.dataset': singleItem(f['data_stream.dataset']),
      containerId,
      podUid,
      hostname,
      hosthostname,
      ...findParent({ containerId, podUid, hostname, hosthostname })
    };
  });

  try {
    const updatedResults = [...resultsAcc, ...processed];
    const lastHit = response.hits.hits.slice(-1)[0];

    if (
      typeof response.hits.total !== "number"
      && response.hits.total
      && response.hits.total.value > response.hits.hits.length
      && response.hits.hits.length > 0
      && lastHit
    ) {
      return await batchSearchForServiceParents({ esClient, services, searchAfter: lastHit.sort, resultsAcc: updatedResults });
    }

    // const deduped = updatedResults.reduce<ServiceParentResult[]>((all, next) => {
    //   const exists = all.find((s) => s.name === next.name && s.environment === next.environment);
    //   if (exists) {
    //     exists['@timestamp'] = next['@timestamp'];
    //   } else {
    //     all.push(next);
    //   }
    //   return all;
    // }, []);

    return { services: updatedResults, lastSort: lastHit && lastHit.sort ? lastHit.sort : searchAfter };

  } catch (error: any) {
    console.log("An error occurred!", error.message);
    console.log('\nQUERY:');
    console.log(JSON.stringify(dsl));
    console.log('\nLAST HIT:');
    const lastHit = response.hits.hits.slice(-1);
    console.log(JSON.stringify(lastHit));
    console.log('\nRESPONSE:');
    console.log(`${response.hits.hits.length} hits returned`);
    response.hits.hits = [];
    console.log(JSON.stringify(response));

    throw error;
  }
  
}

export async function collectServicesFromSummaries({ esClient }: { esClient: AssetClient }) {
  const { services } = await batchSearchForServices({ esClient });
  const { services: fullServices } = await batchSearchForServiceParents({ esClient, services });
  return { services, fullServices }
}

async function searchAndPrintQueryOnError(esClient: AssetClient, dsl: SearchRequest) {
  try {
    return await esClient.search(dsl);
  } catch (error: any) {
    console.log('Error while querying ES', error.message);
    console.log(JSON.stringify(dsl));
    throw error;
  }
}