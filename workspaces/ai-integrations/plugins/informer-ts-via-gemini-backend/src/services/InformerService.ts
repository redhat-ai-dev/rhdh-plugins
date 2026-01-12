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

// Constants for condition types (matching Go constants from bridgerest package)
const INF_SVC_IngressReady_CONDITION = 'IngressReady';
const INF_SVC_PredictorReady_CONDITION = 'PredictorReady';
const INF_SVC_Ready_CONDITION = 'Ready';

// Label constants for KFMR-managed InferenceServices
const INF_SVC_RM_ID_LABEL = 'modelregistry.opendatahub.io/registered-model-id';
const INF_SVC_MV_ID_LABEL = 'modelregistry.opendatahub.io/model-version-id';
// const INF_SVC_INF_SVC_ID_LABEL =
//  'modelregistry.opendatahub.io/inference-service-id';

// Normalizer types
enum NormalizerType {
  KubeflowNormalizer = 'kubeflow',
  KServeNormalizer = 'kserve',
}

// Define the type for the InferenceService object with detailed status
interface Condition {
  type: string;
  status: string;
  lastTransitionTime?: string;
  reason?: string;
  message?: string;
}

interface ModelStatus {
  transitionStatus?: string;
  states?: any;
}

interface InferenceServiceStatus {
  conditions?: Condition[];
  modelStatus?: ModelStatus;
  url?: string;
  address?: {
    url: string;
  };
}

interface InferenceService {
  apiVersion: string;
  kind: string;
  metadata: {
    name: string;
    namespace: string;
    uid?: string;
    labels?: { [key: string]: string };
    annotations?: { [key: string]: string };
  };
  spec: any;
  status?: InferenceServiceStatus;
}

// Configuration (these would typically come from environment variables)
interface ReconcilerConfig {
  kfmrRegistryRoutes: Map<string, any>; // KFMR routes
  storageURL: string;
  format: string;
  defaultLifecycle: string;
  defaultOwner: string;
}

// Helper function to sanitize names (matching Go util.SanitizeName)
function sanitizeName(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9-]/g, '-');
}

// Helper function to build import key (matching Go util.BuildImportKeyAndURI)
function buildImportKeyAndURI(
  namespace: string,
  name: string,
  // format: string,
): [string, string] {
  const sanitizedNs = sanitizeName(namespace);
  const sanitizedName = sanitizeName(name);
  const importKey = `${sanitizedNs}/${sanitizedName}`;
  const uri = `/models/${importKey}`;
  return [importKey, uri];
}

// Helper function to check if InferenceService status is ready
function isInferenceServiceReady(is: InferenceService): boolean {
  if (!is.status) {
    console.log(
      `InferenceService ${is.metadata.namespace}/${is.metadata.name} has no status`,
    );
    return false;
  }

  // Check if conditions exist
  if (!is.status.conditions || is.status.conditions.length === 0) {
    console.log(
      `InferenceService ${is.metadata.namespace}/${is.metadata.name} has no conditions`,
    );
    return false;
  }

  // Check model status transition status
  if (is.status.modelStatus?.transitionStatus !== 'UpToDate') {
    console.log(
      `InferenceService ${is.metadata.namespace}/${is.metadata.name} transitionStatus is not UpToDate: ${is.status.modelStatus?.transitionStatus}`,
    );
    return false;
  }

  // Check required conditions
  for (const condition of is.status.conditions) {
    if (
      condition.type === INF_SVC_IngressReady_CONDITION ||
      condition.type === INF_SVC_PredictorReady_CONDITION ||
      condition.type === INF_SVC_Ready_CONDITION
    ) {
      if (condition.status !== 'True') {
        console.log(
          `InferenceService ${is.metadata.namespace}/${is.metadata.name} condition ${condition.type} is not True: ${condition.status}`,
        );
        return false;
      }
    }
  }

  // Check URL exists
  if (!is.status.url && !is.status.address?.url) {
    console.log(
      `InferenceService ${is.metadata.namespace}/${is.metadata.name} has no URL`,
    );
    return false;
  }

  return true;
}

// Main reconciliation logic (converted from Go Reconcile method starting at line 366)
async function reconcileInferenceService(
  is: InferenceService,
  config: ReconcilerConfig,
): Promise<void> {
  const namespace = is.metadata.namespace;
  const name = is.metadata.name;

  console.log(`Reconciling InferenceService: ${namespace}/${name}`);

  // Variables to track the reconciliation state
  let importKey = '';
  const lastUpdateTimeSinceEpoch = '';
  const modelCardKey = '';
  // let modelCard: string | undefined;
  let normalizerType = NormalizerType.KubeflowNormalizer;

  // Step 1: Process KFMR if routes are available (line 365-369 in Go)
  if (config.kfmrRegistryRoutes.size > 0) {
    console.log(`Processing KFMR for ${namespace}/${name}`);
    // TODO: Implement processKFMR logic
    // This would call the KFMR API to get model registry information
    // For now, we'll skip this and assume KServe-only mode
    // const result = await processKFMR(namespace, name, is, config);
    // if (result) {
    //   importKey = result.importKey;
    //   lastUpdateTimeSinceEpoch = result.lastUpdateTimeSinceEpoch;
    //   modelCardKey = result.modelCardKey;
    //   modelCard = result.modelCard;
    // }
  }

  // Step 2: Handle KServe-only scenario if no importKey (lines 371-408 in Go)
  if (!importKey) {
    console.log(`Processing KServe-only mode for ${namespace}/${name}`);
    normalizerType = NormalizerType.KServeNormalizer;

    // Wait for status to reach a functional, ready state
    if (!isInferenceServiceReady(is)) {
      console.log(
        `InferenceService ${namespace}/${name} is not ready yet, will retry later`,
      );
      // In a real implementation, this would requeue the reconciliation
      return;
    }

    // Call backstage printers (equivalent to kserve.CallBackstagePrinters in Go)
    console.log(`Calling backstage printers for ${namespace}/${name}`);
    // TODO: Implement CallBackstagePrinters logic
    // const catalogData = await callBackstagePrinters(namespace, config.defaultLifecycle, is, config.format);

    // Build import key
    [importKey] = buildImportKeyAndURI(namespace, name); // , config.format);
    console.log(`Built importKey: ${importKey}`);
  }

  // Step 3: Process buffer and send to storage (lines 410-413 in Go)
  console.log(
    `Processing buffer for ${namespace}/${name} with importKey: ${importKey}`,
  );
  await processBWriter(
    importKey,
    normalizerType,
    lastUpdateTimeSinceEpoch,
    modelCardKey,
    // modelCard,
    config,
  );

  console.log(`Successfully reconciled InferenceService: ${namespace}/${name}`);
}

// Helper function to process buffer and send to storage (matching Go processBWriter)
async function processBWriter(
  importKey: string,
  normalizerType: NormalizerType,
  lastUpdateTimeSinceEpoch: string,
  modelCardKey: string,
  // modelCard: string | undefined,
  config: ReconcilerConfig,
): Promise<void> {
  console.log(
    `processBWriter - key: ${importKey}, type: ${normalizerType}, epoch: ${lastUpdateTimeSinceEpoch}, modelCardKey: ${modelCardKey}`,
  );

  // TODO: Implement storage client call
  // This would make an HTTP request to the storage service
  // const response = await storageClient.upsertModel(
  //   importKey,
  //   normalizerType,
  //   lastUpdateTimeSinceEpoch,
  //   modelCardKey,
  //   modelCard,
  //   bufferData
  // );
  //
  // if (response.status !== 200 && response.status !== 201) {
  //   throw new Error(`Storage returned status ${response.status}: ${response.message}`);
  // }

  console.log(`Would send to storage at: ${config.storageURL}`);
}

// Helper function to post current key set to storage
async function postCurrentKeySet(
  keys: string[],
  config: ReconcilerConfig,
): Promise<void> {
  console.log(`Posting current key set to storage: ${keys.length} keys`, keys);

  // TODO: Implement actual HTTP POST to storage service
  // const response = await fetch(`${config.storageURL}/api/v1/keys`, {
  //   method: 'POST',
  //   headers: { 'Content-Type': 'application/json' },
  //   body: JSON.stringify({ keys }),
  // });
  //
  // if (response.status !== 200 && response.status !== 201) {
  //   throw new Error(`Storage returned status ${response.status}`);
  // }

  console.log(`Would post key set to storage at: ${config.storageURL}`);
}

// Main polling/sync function (converted from Go innerStart method starting at line 651)
// This is called on delete events and during background polling to sync the current state
async function innerStart(
  client: k8s.CustomObjectsApi,
  // coreClient: k8s.CoreV1Api,
  config: ReconcilerConfig,
): Promise<void> {
  console.log('innerStart: Beginning reconciliation sync');

  // TODO: Call setupKFMR to ensure KFMR routes are configured
  // In the Go code, this is line 652

  const keys: string[] = [];

  // Step 1: Process KFMR registries (lines 658-794 in Go)
  if (config.kfmrRegistryRoutes.size > 0) {
    console.log(
      `innerStart: Processing ${config.kfmrRegistryRoutes.size} KFMR registries`,
    );

    // TODO: Implement KFMR processing loop
    // This would:
    // 1. Loop over each KFMR registry (line 658)
    // 2. Call LoopOverKFMR to get registered models, model versions, and model artifacts (line 664)
    // 3. For each registered model:
    //    a. Get its model versions (line 671)
    //    b. Get its model artifacts (line 676)
    //    c. For each model version:
    //       - Build import key (line 684)
    //       - Get KubeFlow inference services for the model version (line 701)
    //       - If no KubeFlow inference services, call backstage printers without KServe (line 707-722)
    //       - If there are KubeFlow inference services:
    //         * Check if deployed (line 730)
    //         * Find matching KServe inference service by labels (line 736-753)
    //         * Call backstage printers with both KubeFlow and KServe (line 759-776)
    //       - Add import key to keys array (line 690)
    //
    // For now, we'll skip KFMR processing and focus on KServe-only scenario
  }

  // Step 2: List all KServe InferenceServices (lines 796-824 in Go)
  console.log('innerStart: Listing all KServe InferenceServices');

  try {
    const response = await client.listClusterCustomObject(
      group,
      version,
      plural,
    );

    const inferenceServices = (response.body as any)
      .items as InferenceService[];
    console.log(
      `innerStart: Found ${inferenceServices.length} KServe InferenceServices`,
    );

    for (const is of inferenceServices) {
      let skip = false;

      // Skip InferenceServices managed by KubeFlow (lines 803-816 in Go)
      if (is.metadata.labels && config.kfmrRegistryRoutes.size > 0) {
        for (const labelKey of Object.keys(is.metadata.labels)) {
          if (
            labelKey === INF_SVC_MV_ID_LABEL ||
            labelKey === INF_SVC_RM_ID_LABEL
          ) {
            console.log(
              `innerStart: Skipping InferenceService ${is.metadata.namespace}/${is.metadata.name} since it is managed by KubeFlow`,
            );
            skip = true;
            break;
          }
        }
      }

      if (!skip) {
        // Build import key for KServe-only InferenceService (line 819)
        const [importKey] = buildImportKeyAndURI(
          is.metadata.namespace,
          is.metadata.name,
        );
        console.log(
          `innerStart: Adding importKey ${importKey} for KServe InferenceService ${is.metadata.namespace}/${is.metadata.name}`,
        );
        keys.push(importKey);
      }
    }
  } catch (error) {
    console.error('innerStart: Error listing KServe InferenceServices:', error);
  }

  // Step 3: Post current key set to storage (lines 826-835 in Go)
  try {
    await postCurrentKeySet(keys, config);
    console.log(
      `innerStart: Successfully posted ${keys.length} keys to storage`,
    );
  } catch (error) {
    console.error('innerStart: Error posting current key set:', error);
  }

  console.log('innerStart: Reconciliation sync complete');
}

export const setupInformer = async () => {
  const kc = new k8s.KubeConfig();
  kc.loadFromDefault();

  const client = kc.makeApiClient(k8s.CustomObjectsApi);
  // const coreClient = kc.makeApiClient(k8s.CoreV1Api);

  // Initialize configuration from environment variables
  const config: ReconcilerConfig = {
    kfmrRegistryRoutes: new Map(), // TODO: Initialize from env var MODEL_REGISTRY_ROUTE
    storageURL: process.env.STORAGE_URL || 'http://localhost:7070',
    format: process.env.FORMAT || 'catalog-info.yaml',
    defaultLifecycle: process.env.LIFECYCLE || 'production',
    defaultOwner: process.env.OWNER || 'default-owner',
  };

  console.log('Reconciler configuration:', {
    storageURL: config.storageURL,
    format: config.format,
    defaultLifecycle: config.defaultLifecycle,
    defaultOwner: config.defaultOwner,
    kfmrRegistryRoutes: config.kfmrRegistryRoutes.size,
  });

  const listFn: k8s.ListPromise<InferenceService> = () =>
    client.listClusterCustomObject(group, version, plural) as any;

  const informerInstance = k8s.makeInformer(
    kc,
    `/apis/${group}/${version}/${plural}`,
    listFn,
  );

  informerInstance.on('add', async (obj: InferenceService) => {
    console.log(
      `Added: ${obj.metadata.name} in namespace ${obj.metadata.namespace}`,
    );

    // Execute the reconciliation logic (converted from Go Reconcile method)
    try {
      await reconcileInferenceService(obj, config);
    } catch (error) {
      console.error(
        `Error reconciling InferenceService ${obj.metadata.namespace}/${obj.metadata.name}:`,
        error,
      );
    }
  });

  informerInstance.on('update', async (obj: InferenceService) => {
    console.log(
      `Updated: ${obj.metadata.name} in namespace ${obj.metadata.namespace}`,
    );

    // Execute the reconciliation logic for updates as well
    try {
      await reconcileInferenceService(obj, config);
    } catch (error) {
      console.error(
        `Error reconciling InferenceService ${obj.metadata.namespace}/${obj.metadata.name}:`,
        error,
      );
    }
  });

  informerInstance.on('delete', async (obj: InferenceService) => {
    console.log(
      `Deleted: ${obj.metadata.name} in namespace ${obj.metadata.namespace}`,
    );

    // Delete processing: Call innerStart to sync the current state (Go code line 339-351)
    // This will:
    // 1. Poll KFMR to remove URLs/routes from model entries that depended on this InferenceService
    // 2. If the delete resulted from archiving, remove the model from storage
    // 3. Update the current key set to reflect the deletion
    try {
      console.log(
        `Initiating delete processing for ${obj.metadata.namespace}/${obj.metadata.name}`,
      );
      await innerStart(client, /* coreClient,*/ config);
      console.log(
        `Delete processing completed for ${obj.metadata.namespace}/${obj.metadata.name}`,
      );
    } catch (error) {
      console.error(
        `Error during delete processing for ${obj.metadata.namespace}/${obj.metadata.name}:`,
        error,
      );
    }
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

  // Optional: Start background polling to supplement the informer
  // This matches the Go Start method (lines 639-649)
  // The controller relist does not duplicate delete events, so background polling
  // provides more fine-grained control over what we attempt to relist
  const pollingInterval = parseInt(
    process.env.POLLING_INTERVAL || '120000',
    10,
  ); // Default 2 minutes

  if (pollingInterval > 0) {
    console.log(
      `Starting background polling every ${pollingInterval / 1000} seconds`,
    );
    const pollingTimer = setInterval(async () => {
      try {
        console.log('Background polling: Calling innerStart');
        await innerStart(client, /* coreClient,*/ config);
      } catch (error) {
        console.error('Background polling: Error during innerStart:', error);
      }
    }, pollingInterval);

    // Store the timer in case we need to stop it later
    (informerInstance as any).__pollingTimer = pollingTimer;
  }

  return informerInstance;
};
