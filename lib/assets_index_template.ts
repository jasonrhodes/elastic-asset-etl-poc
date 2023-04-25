import { IndicesPutIndexTemplateRequest } from '@elastic/elasticsearch/lib/api/types';

export const assetsIndexTemplateConfig: IndicesPutIndexTemplateRequest = {
  name: 'assets',
  index_patterns: ['assets*'],
  priority: 100,
  template: {
    settings: {},
    mappings: {
      // subobjects appears to not exist in the types, but is a valid ES mapping option
      // see: https://www.elastic.co/guide/en/elasticsearch/reference/master/subobjects.html
      // @ts-ignore
      subobjects: false,
      dynamic_templates: [
        {
          strings_as_keywords: {
            mapping: {
              ignore_above: 1024,
              type: 'keyword',
            },
            match_mapping_type: 'string',
          },
        },
      ],
      properties: {
        '@timestamp': {
          type: 'date',
        },
      },
    },
  },
};
