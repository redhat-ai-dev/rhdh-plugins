/*
 * Copyright Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  processingResult,
  CatalogProcessor,
  CatalogProcessorEmit,
  CatalogProcessorParser,
  CatalogProcessorResult,
} from '@backstage/plugin-catalog-node';
import { UrlReaderService } from '@backstage/backend-plugin-api';

import { LocationSpec } from '@backstage/plugin-catalog-common';

// A processor that reads from the RHDH RHOAI Bridge
export class RHDHRHOAIReaderProcessor implements CatalogProcessor {
  constructor(private readonly reader: UrlReaderService) {}

  getProcessorName(): string {
    return 'RHDHRHOAIReaderProcessor';
  }

  async readLocation(
    location: LocationSpec,
    _optional: boolean,
    emit: CatalogProcessorEmit,
    parser: CatalogProcessorParser,
  ): Promise<boolean> {
    // Pick a custom location type string. A location will be
    // registered later with this type.
    if (location.type !== 'rhdh-rhoai-bridge') {
      return false;
    }

    try {
      // Use the builtin reader facility to grab data from the
      // API. If you prefer, you can just use plain fetch here
      // (from the node-fetch package), or any other method of
      // your choosing.
      // TODO eventually we will want to take the k8s credentials provided to
      // backstage and its k8s plugin and supply them to the bridge
      // for potential auth and access control checks (i.e. SARs) with the kubeflow MR
      const data = await this.reader.readUrl(location.target);
      const response = [{ url: location.target, data: await data.buffer() }];
      // Repeatedly call emit(processingResult.entity(location, <entity>))
      const parseResults: CatalogProcessorResult[] = [];
      for (const item of response) {
        for await (const parseResult of parser({
          data: item.data,
          location: { type: location.type, target: item.url },
        })) {
          parseResults.push(parseResult);
          emit(parseResult);
        }
      }
      emit(processingResult.refresh(`${location.type}:${location.target}`));
    } catch (error) {
      const message = `Unable to read ${location.type}, ${error}`.substring(
        0,
        5000,
      );
      // TODO when we enable cache this is how UrlReaderPRocessor.ts in core backstage handles errors
      // if (error.name === 'NotModifiedError' && cacheItem) {
      //   for (const parseResult of cacheItem.value) {
      //     emit(parseResult);
      //   }
      //   emit(processingResult.refresh(`${location.type}:${location.target}`));
      //   await cache.set(CACHE_KEY, cacheItem);
      // } else if (error.name === 'NotFoundError') {
      //   if (!optional) {
      //     emit(processingResult.notFoundError(location, message));
      //   }
      // } else {
      //   emit(processingResult.generalError(location, message));
      // }
      emit(processingResult.generalError(location, message));
    }

    return true;
  }
}
