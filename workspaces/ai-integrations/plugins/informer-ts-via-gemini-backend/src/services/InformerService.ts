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
import { callBackstagePrinters, type ModelCatalog, setupKFMR } from './Kfmr';
import {
  callBackstagePrinters as callKServeBackstagePrinters,
  type KServeModelCatalog,
} from './KServe';

const group = 'serving.kserve.io';
const version = 'v1beta1';
const plural = 'inferenceservices';

// Model card metadata interface (from server.go line 30-35)
interface ModelCardMetadata {
  content: string;
  lastUpdateTimeSinceEpoch: string;
  updateCount: number;
  needToUpdate: boolean;
}

// Global model cards storage (from server.go line 23)
// This stores model card content indexed by modelCardKey
const modelCards = new Map<string, ModelCardMetadata>();

// Model catalog metadata interface
interface ModelCatalogMetadata {
  catalogData: ModelCatalog | KServeModelCatalog;
  lastUpdateTimeSinceEpoch: string;
  normalizerType: NormalizerType;
  updateCount: number;
  needToUpdate: boolean;
}

// Global model catalog storage
// This stores model catalog data indexed by importKey
const modelCatalog = new Map<string, ModelCatalogMetadata>();

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

export interface RouteIngress {
  host: string;
}

export interface RouteStatus {
  ingress?: RouteIngress[];
}

export interface Route {
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
  status?: RouteStatus;
}

// KFMR (KubeFlow Model Registry) related interfaces
interface RegisteredModel {
  id?: string;
  name: string;
  lastUpdateTimeSinceEpoch?: string;
  description?: string;
  customProperties?: { [key: string]: any };
}

interface ModelVersion {
  id?: string;
  name: string;
  lastUpdateTimeSinceEpoch?: string;
  registeredModelId?: string;
  description?: string;
  customProperties?: { [key: string]: any };
}

interface ModelArtifact {
  id?: string;
  name?: string;
  modelVersionId?: string;
  modelSourceClass?: string;
  modelSourceGroup?: string;
  modelSourceName?: string;
  uri?: string;
  customProperties?: { [key: string]: any };
}

interface KFMRInferenceService {
  id?: string;
  name?: string;
  registeredModelId?: string;
  modelVersionId?: string;
  servingEnvironmentId?: string;
  desiredState?: string;
  runtime?: string;
  customProperties?: { [key: string]: any };
}

interface ServingEnvironment {
  id?: string;
  name?: string;
  description?: string;
  customProperties?: { [key: string]: any };
}

// KFMR Client wrapper interface
interface KFMRClient {
  rootRegistryURL: string;
  rootCatalogURL?: string;
  token: string;
  listRegisteredModels(): Promise<RegisteredModel[]>;
  listInferenceServices(): Promise<KFMRInferenceService[]>;
  listModelVersions(registeredModelId: string): Promise<ModelVersion[]>;
  listModelArtifacts(modelVersionId: string): Promise<ModelArtifact[]>;
  getServingEnvironment(
    servingEnvironmentId: string,
  ): Promise<ServingEnvironment>;
  getModelVersion(modelVersionId: string): Promise<ModelVersion>;
  getModelCard(
    modelSourceClass: string,
    modelSourceGroup: string,
    modelSourceName: string,
  ): Promise<string | undefined>;
}

// Result type for processKFMR
interface ProcessKFMRResult {
  importKey: string;
  lastUpdateTimeSinceEpoch: string;
  modelCardKey: string;
  modelCard?: string;
  catalogData: ModelCatalog;
}

// Configuration (these would typically come from environment variables)
export interface ReconcilerConfig {
  kfmrClients: Map<string, KFMRClient>; // KFMR clients keyed by registry identifier
  kfmrRoutes: Map<string, Route>; // KFMR routes keyed by registry identifier
  kfmrCatalogRoute?: Route; // KFMR catalog route
  storageURL: string;
  defaultLifecycle: string;
  defaultOwner: string;
  k8sToken?: string; // Kubernetes authentication token
  routeClient?: any; // OpenShift route client (TODO: add proper type)
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

// Helper function to check if KServe InferenceService maps to KFMR model
// Converted from Go util.KServeInferenceServiceMapping (utils.go line 123)
function kserveInferenceServiceMapping(
  registeredModelId: string,
  modelVersionId: string,
  is: InferenceService,
): boolean {
  // Check if labels exist (Go line 124-126)
  if (!is.metadata.labels) {
    return false;
  }

  // Check registered model ID label (Go line 128-135)
  const rmVal = is.metadata.labels[INF_SVC_RM_ID_LABEL];
  if (!rmVal) {
    return false;
  }

  if (registeredModelId.trim() !== rmVal.trim()) {
    return false;
  }

  // Check model version ID label (Go line 137-144)
  const mvVal = is.metadata.labels[INF_SVC_MV_ID_LABEL];
  if (!mvVal) {
    return false;
  }

  if (modelVersionId.trim() !== mvVal.trim()) {
    return false;
  }

  // All checks passed (Go line 146)
  return true;
}

// Process KFMR (KubeFlow Model Registry) integration
// Converted from Go processKFMR method (controller.go line 442)
async function processKFMR(
  namespace: string,
  name: string,
  is: InferenceService,
  config: ReconcilerConfig,
): Promise<ProcessKFMRResult | null> {
  console.log(`processKFMR: Processing ${namespace}/${name}`);

  // Check if KFMR is configured (Go line 444-448)
  if (config.kfmrClients.size === 0) {
    console.log(
      `processKFMR: No KFMR routes configured for ${namespace}/${name}`,
    );
    return null;
  }

  console.log(`processKFMR: Have KFMR clients for ${namespace}/${name}`);
  const replacer = (str: string) => str.replace(/ /g, ''); // Go line 451

  const kfmrRMs: RegisteredModel[] = [];
  const kfmrISs: KFMRInferenceService[] = [];

  // Loop over KFMR clients (Go line 455)
  for (const [key, kfmr] of config.kfmrClients.entries()) {
    try {
      // List registered models (Go line 456)
      const rms = await kfmr.listRegisteredModels();
      console.log(
        `processKFMR: Found ${rms.length} registered models with registry ${key}`,
      );
      kfmrRMs.push(...rms);

      // List inference services (Go line 464)
      const iss = await kfmr.listInferenceServices();
      console.log(
        `processKFMR: Found ${iss.length} inference services from ${key}`,
      );
      kfmrISs.push(...iss);
    } catch (error) {
      console.error(
        `processKFMR: Error listing models or services for ${namespace}/${name}:`,
        error,
      );
      continue;
    }

    // Path 1: No KubeFlow inference services - match by model/version name (Go line 472)
    if (kfmrISs.length === 0) {
      console.log(
        `processKFMR: No KubeFlow inference services for registry ${key}`,
      );

      for (const rm of kfmrRMs) {
        if (!rm.id) continue;

        try {
          // List model versions (Go line 476)
          const mvs = await kfmr.listModelVersions(rm.id);
          console.log(
            `processKFMR: Found ${mvs.length} model versions for registered model ${rm.id}`,
          );

          for (const mv of mvs) {
            if (!mv.id) continue;

            // Check if KServe InferenceService maps to this model version (Go line 482)
            if (kserveInferenceServiceMapping(rm.id, mv.id, is)) {
              console.log(
                `processKFMR: Found mapping between KServe IS and model version ${mv.id}`,
              );

              // Get model artifacts (Go line 485)
              let mas: ModelArtifact[] = [];
              try {
                mas = await kfmr.listModelArtifacts(mv.id);
                console.log(
                  `processKFMR: Found ${mas.length} model artifacts for model version ${mv.id}`,
                );
              } catch (error) {
                console.error(
                  `processKFMR: Error getting model artifacts for ${mv.id}:`,
                  error,
                );
              }

              if (!mas || mas.length === 0) {
                console.log(
                  `processKFMR: No model artifacts, bypassing backstage printers`,
                );
                continue;
              }

              // Call backstage printers (Go line 497)
              const catalogData: ModelCatalog = await callBackstagePrinters(
                config.defaultOwner,
                config.defaultLifecycle,
                rm,
                mv,
                mas,
                // null,
                is,
              );
              console.log(
                `processKFMR: Generated catalog data with ${catalogData.models.length} models and ${catalogData.modelServers.length} model servers`,
              );

              // Build import key (Go line 515)
              const [importKey] = buildImportKeyAndURI(
                sanitizeName(rm.name),
                sanitizeName(mv.name),
              );

              // Get last update timestamp (Go line 516)
              let lastUpdateTimeSinceEpoch = mv.lastUpdateTimeSinceEpoch || '';
              if (
                rm.lastUpdateTimeSinceEpoch &&
                rm.lastUpdateTimeSinceEpoch > lastUpdateTimeSinceEpoch
              ) {
                lastUpdateTimeSinceEpoch = rm.lastUpdateTimeSinceEpoch;
              }

              // Get model card if catalog URL exists (Go line 522)
              let modelCard: string | undefined;
              let modelCardKey = '';
              if (kfmr.rootCatalogURL) {
                for (const ma of mas) {
                  if (
                    ma.modelSourceClass &&
                    ma.modelSourceGroup &&
                    ma.modelSourceName
                  ) {
                    try {
                      modelCard = await kfmr.getModelCard(
                        ma.modelSourceClass,
                        ma.modelSourceGroup,
                        ma.modelSourceName,
                      );
                      modelCardKey =
                        replacer(ma.modelSourceClass) +
                        replacer(ma.modelSourceGroup) +
                        replacer(ma.modelSourceName);
                      console.log(
                        `processKFMR: Built modelCardKey ${modelCardKey}`,
                      );
                      break;
                    } catch (error) {
                      console.error(
                        `processKFMR: Error getting model card:`,
                        error,
                      );
                    }
                  }
                }
              }

              return {
                importKey,
                lastUpdateTimeSinceEpoch,
                modelCardKey,
                modelCard,
                catalogData,
              };
            }
          }
        } catch (error) {
          console.error(
            `processKFMR: Error listing model versions for ${rm.id}:`,
            error,
          );
        }
      }
    }

    // Path 2: Found KubeFlow inference services - match with KServe (Go line 542)
    console.log(
      `processKFMR: Found KubeFlow inference services while processing KServe IS ${namespace}/${name}`,
    );

    for (const rm of kfmrRMs) {
      if (!rm.id) {
        console.log(
          `processKFMR: Registered model ${rm.name} has no ID, skipping`,
        );
        continue;
      }

      for (const kfmrIS of kfmrISs) {
        // Check if KubeFlow IS matches registered model and KServe IS name (Go line 551)
        if (
          kfmrIS.id &&
          kfmrIS.registeredModelId === rm.id &&
          kfmrIS.name &&
          is.metadata.name.startsWith(kfmrIS.name)
        ) {
          console.log(
            `processKFMR: KServe IS name match, checking namespace ${namespace} and serving environment ${kfmrIS.servingEnvironmentId}`,
          );

          if (!kfmrIS.servingEnvironmentId) continue;

          // Get serving environment (Go line 556)
          let se: ServingEnvironment;
          try {
            se = await kfmr.getServingEnvironment(kfmrIS.servingEnvironmentId);
          } catch (error) {
            console.error(
              `processKFMR: Error getting serving environment ${kfmrIS.servingEnvironmentId}:`,
              error,
            );
            continue;
          }

          // Check if serving environment name matches namespace (Go line 562)
          if (se.name === namespace) {
            console.log(`processKFMR: Matched KServe IS ${namespace}/${name}`);

            if (!kfmrIS.modelVersionId) continue;

            // Get model version (Go line 569)
            let mv: ModelVersion;
            try {
              mv = await kfmr.getModelVersion(kfmrIS.modelVersionId);
            } catch (error) {
              console.error(
                `processKFMR: Error getting model version ${kfmrIS.modelVersionId}:`,
                error,
              );
              continue;
            }

            // Get model artifacts (Go line 574)
            let mas: ModelArtifact[] = [];
            try {
              mas = await kfmr.listModelArtifacts(kfmrIS.modelVersionId);
            } catch (error) {
              console.error(
                `processKFMR: Error getting model artifacts for ${kfmrIS.modelVersionId}:`,
                error,
              );
            }

            if (!mv || !mas || mas.length === 0) {
              console.log(
                `processKFMR: Missing model version or artifacts, bypassing backstage printers`,
              );
              continue;
            }

            // Call backstage printers (Go line 585)
            const catalogData: ModelCatalog = await callBackstagePrinters(
              config.defaultOwner,
              config.defaultLifecycle,
              rm,
              mv,
              mas,
              // kfmrIS,
              is,
            );
            console.log(
              `processKFMR: Generated catalog data with ${catalogData.models.length} models and ${catalogData.modelServers.length} model servers`,
            );

            // Build import key (Go line 602)
            const [importKey] = buildImportKeyAndURI(
              sanitizeName(rm.name),
              sanitizeName(mv.name),
            );

            // Get last update timestamp (Go line 603)
            let lastUpdateTimeSinceEpoch = mv.lastUpdateTimeSinceEpoch || '';
            if (
              rm.lastUpdateTimeSinceEpoch &&
              rm.lastUpdateTimeSinceEpoch > lastUpdateTimeSinceEpoch
            ) {
              lastUpdateTimeSinceEpoch = rm.lastUpdateTimeSinceEpoch;
            }

            // Get model card if catalog URL exists (Go line 609)
            let modelCard: string | undefined;
            let modelCardKey = '';
            if (kfmr.rootCatalogURL) {
              for (const ma of mas) {
                if (
                  ma.modelSourceClass &&
                  ma.modelSourceGroup &&
                  ma.modelSourceName
                ) {
                  try {
                    modelCard = await kfmr.getModelCard(
                      ma.modelSourceClass,
                      ma.modelSourceGroup,
                      ma.modelSourceName,
                    );
                    modelCardKey = ma.modelSourceGroup + ma.modelSourceName;
                    console.log(
                      `processKFMR: Built modelCardKey ${modelCardKey}`,
                    );
                    break;
                  } catch (error) {
                    console.error(
                      `processKFMR: Error getting model card:`,
                      error,
                    );
                  }
                }
              }
            }

            console.log(
              `processKFMR: KServe IS ${namespace}/${name} returning importKey ${importKey}`,
            );
            return {
              importKey,
              lastUpdateTimeSinceEpoch,
              modelCardKey,
              modelCard,
              catalogData,
            };
          }
        }
      }
    }
  }

  // No match found, but not an error - caller can process as KServe-only (Go line 633)
  console.log(`processKFMR: No KFMR match for ${namespace}/${name}`);
  return null;
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
  let lastUpdateTimeSinceEpoch = '';
  let modelCardKey = '';
  let modelCard: string | undefined;
  let catalogData: ModelCatalog | KServeModelCatalog | undefined;
  let normalizerType = NormalizerType.KubeflowNormalizer;

  // Step 1: Process KFMR if routes are available (line 365-369 in Go)
  if (config.kfmrClients.size > 0) {
    console.log(`Processing KFMR for ${namespace}/${name}`);
    const result = await processKFMR(namespace, name, is, config);
    if (result) {
      importKey = result.importKey;
      lastUpdateTimeSinceEpoch = result.lastUpdateTimeSinceEpoch;
      modelCardKey = result.modelCardKey;
      modelCard = result.modelCard;
      catalogData = result.catalogData;
    }
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
    catalogData = await callKServeBackstagePrinters(
      config.defaultOwner,
      config.defaultLifecycle,
      is,
    );
    console.log(
      `Generated KServe catalog data with ${catalogData.models.length} models and ${catalogData.modelServers.length} model servers`,
    );

    // Build import key
    [importKey] = buildImportKeyAndURI(namespace, name);
    console.log(`Built importKey: ${importKey}`);
  }

  // Step 3: Process buffer and send to storage (lines 410-413 in Go)
  if (!catalogData) {
    console.error(
      `No catalog data available for ${namespace}/${name}, skipping processModelCatalog`,
    );
    return;
  }

  console.log(
    `Processing buffer for ${namespace}/${name} with importKey: ${importKey}`,
  );
  await processModelCatalog(
    importKey,
    normalizerType,
    lastUpdateTimeSinceEpoch,
    modelCardKey,
    modelCard,
    catalogData,
  );

  console.log(`Successfully reconciled InferenceService: ${namespace}/${name}`);
}

// Helper function to process buffer and send to storage (matching Go processBWriter)
async function processModelCatalog(
  importKey: string,
  normalizerType: NormalizerType,
  lastUpdateTimeSinceEpoch: string,
  modelCardKey: string,
  modelCard: string | undefined,
  catalogData: ModelCatalog | KServeModelCatalog,
): Promise<void> {
  console.log(
    `processModelCatalog - key: ${importKey}, type: ${normalizerType}, epoch: ${lastUpdateTimeSinceEpoch}, modelCardKey: ${modelCardKey}`,
  );
  console.log(
    `processModelCatalog - catalogData has ${catalogData.models.length} models and ${catalogData.modelServers.length} model servers`,
  );

  // Handle model catalog storage
  if (importKey && importKey.length > 0) {
    const existingCatalog = modelCatalog.get(importKey);

    if (!existingCatalog) {
      // Create new model catalog metadata entry
      const mcm: ModelCatalogMetadata = {
        catalogData: catalogData,
        lastUpdateTimeSinceEpoch: lastUpdateTimeSinceEpoch,
        normalizerType: normalizerType,
        needToUpdate: true,
        updateCount: 0,
      };
      modelCatalog.set(importKey, mcm);
      console.log(
        `processModelCatalog: Created new model catalog entry for key ${importKey}`,
      );
    } else {
      // Update existing model catalog metadata if timestamp changed
      if (
        existingCatalog.lastUpdateTimeSinceEpoch !== lastUpdateTimeSinceEpoch
      ) {
        existingCatalog.lastUpdateTimeSinceEpoch = lastUpdateTimeSinceEpoch;
        existingCatalog.catalogData = catalogData;
        existingCatalog.normalizerType = normalizerType;
        existingCatalog.needToUpdate = true;
        existingCatalog.updateCount = 0;
        modelCatalog.set(importKey, existingCatalog);
        console.log(
          `processModelCatalog: Updated model catalog entry for key ${importKey} (timestamp changed)`,
        );
      } else {
        console.log(
          `processModelCatalog: Model catalog for key ${importKey} already up to date`,
        );
      }
    }
  }

  // Handle model card storage (converted from server.go lines 219-234)
  if (modelCardKey && modelCardKey.length > 0) {
    const existingMcm = modelCards.get(modelCardKey);

    if (!existingMcm) {
      // Create new model card metadata entry
      const mcm: ModelCardMetadata = {
        content: modelCard || '',
        lastUpdateTimeSinceEpoch: lastUpdateTimeSinceEpoch,
        needToUpdate: true,
        updateCount: 0,
      };
      modelCards.set(modelCardKey, mcm);
      console.log(
        `processModelCatalog: Created new model card entry for key ${modelCardKey}`,
      );
    } else {
      // Update existing model card metadata if timestamp changed
      if (existingMcm.lastUpdateTimeSinceEpoch !== lastUpdateTimeSinceEpoch) {
        existingMcm.lastUpdateTimeSinceEpoch = lastUpdateTimeSinceEpoch;
        existingMcm.content = modelCard || existingMcm.content;
        existingMcm.needToUpdate = true;
        existingMcm.updateCount = 0;
        modelCards.set(modelCardKey, existingMcm);
        console.log(
          `processModelCatalog: Updated model card entry for key ${modelCardKey} (timestamp changed)`,
        );
      } else {
        console.log(
          `processModelCatalog: Model card for key ${modelCardKey} already up to date`,
        );
      }
    }
  }
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

  const updConfig = await setupKFMR(config);

  const keys: string[] = [];

  // Step 1: Process KFMR registries (lines 658-794 in Go)
  if (updConfig.kfmrClients.size > 0) {
    console.log(
      `innerStart: Processing ${config.kfmrClients.size} KFMR registries`,
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
      if (is.metadata.labels && config.kfmrClients.size > 0) {
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
    kfmrClients: new Map(),
    kfmrRoutes: new Map(),
    kfmrCatalogRoute: undefined,
    storageURL: process.env.STORAGE_URL || 'http://localhost:7070',
    defaultLifecycle: process.env.LIFECYCLE || 'production',
    defaultOwner: process.env.OWNER || 'default-owner',
    k8sToken: undefined, // TODO: Extract token from kc if needed
    routeClient: undefined, // TODO: Create OpenShift route client if available
  };

  console.log('Reconciler configuration (before setupKFMR):', {
    storageURL: config.storageURL,
    defaultLifecycle: config.defaultLifecycle,
    defaultOwner: config.defaultOwner,
    kfmrClients: config.kfmrClients.size,
  });

  // Setup KFMR clients (equivalent to Go line 263: reconciler.setupKFMR(ctx))
  try {
    await setupKFMR(config);
    console.log(
      `Reconciler configuration (after setupKFMR): KFMR clients initialized: ${config.kfmrClients.size}`,
    );
  } catch (error) {
    console.error('Error setting up KFMR:', error);
  }

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
