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

import * as k8s from '@kubernetes/client-node';

const group = 'serving.kserve.io';
const version = 'v1beta1';
const plural = 'inferenceservices';

// Define the type for the InferenceService object
interface InferenceService {
  apiVersion: string;
  kind: string;
  metadata: {
    name: string;
    namespace: string;
  };
  spec: any;
  status: any;
}

export const setupInformer = async () => {
  const kc = new k8s.KubeConfig();
  kc.loadFromDefault();

  const client = kc.makeApiClient(k8s.CustomObjectsApi);

  const listFn: k8s.ListPromise<InferenceService> = () =>
    client.listClusterCustomObject(group, version, plural) as any;

  const informerInstance = k8s.makeInformer(
    kc,
    `/apis/${group}/${version}/${plural}`,
    listFn,
  );

  informerInstance.on('add', (obj: InferenceService) => {
    console.log(
      `Added: ${obj.metadata.name} in namespace ${obj.metadata.namespace}`,
    );
  });

  informerInstance.on('update', (obj: InferenceService) => {
    console.log(
      `Updated: ${obj.metadata.name} in namespace ${obj.metadata.namespace}`,
    );
  });

  informerInstance.on('delete', (obj: InferenceService) => {
    console.log(
      `Deleted: ${obj.metadata.name} in namespace ${obj.metadata.namespace}`,
    );
  });

  informerInstance.on('error', (err: any) => {
    console.error('Informer error:', err);
    // Restart informer after a delay
    setTimeout(() => {
      informerInstance.start();
    }, 5000);
  });

  console.log('Starting informer for InferenceServices...');
  await informerInstance.start();
  console.log('Informer started.');

  return informerInstance;
};
