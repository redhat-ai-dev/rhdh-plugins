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

// Converted from kfmr.go in model-catalog-bridge

// Constants (from kfmr.go line 26-30)
const TAG_REGEXP = '^[a-z0-9:+#]+(\\-[a-z0-9:+#]+)*$';

// Normalizer formats
export enum NormalizerFormat {
  JsonArrayFormat = 'json-array',
  CatalogInfoYamlFormat = 'catalog-info.yaml',
}

// Custom property keys (from brdgtypes package)
export const PropertyKeys = {
  LicenseKey: 'license',
  TechDocsKey: 'techdocs',
  RHOAIModelCatalogSourceModelVersion:
    'rhoai-model-catalog-source-model-version',
  RHOAIModelCatalogSourceModelKey: 'rhoai-model-catalog-source-model',
  RHOAIModelCatalogRegisteredFromKey: 'rhoai-model-catalog-registered-from',
  RHOAIModelCatalogProviderKey: 'rhoai-model-catalog-provider',
  APITypeKey: 'api-type',
  RHOAIModelRegistryRegisteredFromCatalogRepositoryName:
    'rhoai-model-registry-registered-from-catalog-repository-name',
  RHOAIModelRegistryLastModified: 'last-modified',
  Owner: 'owner',
  Lifecycle: 'lifecycle',
  EthicsKey: 'ethics',
  HowToUseKey: 'how-to-use',
  SupportKey: 'support',
  TrainingKey: 'training',
  UsageKey: 'usage',
  HomepageURLKey: 'homepage-url',
  APISpecKey: 'api-spec',
};

// Metadata value interface (from openapi package)
interface MetadataValue {
  metadataStringValue?: {
    stringValue: string;
  };
  metadataIntValue?: {
    intValue: number;
  };
  metadataBoolValue?: {
    boolValue: boolean;
  };
  metadataDoubleValue?: {
    doubleValue: number;
  };
}

// Model state enums
export enum RegisteredModelState {
  Live = 'LIVE',
  Archived = 'ARCHIVED',
}

export enum ModelVersionState {
  Live = 'LIVE',
  Archived = 'ARCHIVED',
}

export enum InferenceServiceState {
  Deployed = 'DEPLOYED',
  Undeployed = 'UNDEPLOYED',
}

// These interfaces are imported from InformerService.ts
export interface RegisteredModel {
  id?: string;
  name: string;
  lastUpdateTimeSinceEpoch?: string;
  description?: string;
  owner?: string;
  state?: RegisteredModelState;
  customProperties?: { [key: string]: MetadataValue };
}

export interface ModelVersion {
  id?: string;
  name: string;
  lastUpdateTimeSinceEpoch?: string;
  registeredModelId?: string;
  description?: string;
  state?: ModelVersionState;
  customProperties?: { [key: string]: MetadataValue };
}

export interface ModelArtifact {
  id?: string;
  name?: string;
  modelVersionId?: string;
  modelSourceClass?: string;
  modelSourceGroup?: string;
  modelSourceName?: string;
  uri?: string;
  description?: string;
  customProperties?: { [key: string]: MetadataValue };
}

export interface KFMRInferenceService {
  id?: string;
  name?: string;
  registeredModelId?: string;
  modelVersionId?: string;
  servingEnvironmentId?: string;
  desiredState?: InferenceServiceState;
  runtime?: string;
  customProperties?: { [key: string]: MetadataValue };
}

export interface KServeInferenceService {
  metadata: {
    name: string;
    namespace: string;
    uid?: string;
    labels?: { [key: string]: string };
    annotations?: { [key: string]: string };
  };
  spec: any;
  status?: {
    conditions?: Array<{
      type: string;
      status: string;
    }>;
    url?: string;
    address?: {
      url: string;
    };
  };
}

// Helper function: Extract tags from custom properties
// Converted from getTagsFromCustomProps (kfmr.go line 142)
export function getTagsFromCustomProps(
  lastMod: boolean,
  props: { [key: string]: MetadataValue },
): { [key: string]: string } {
  const tags: { [key: string]: string } = {};
  const regex = new RegExp(TAG_REGEXP);

  for (const [cpk, cpv] of Object.entries(props)) {
    // Skip certain keys (line 146-150)
    if (cpk === PropertyKeys.LicenseKey || cpk === PropertyKeys.TechDocsKey) {
      console.log('Skip adding TechDocs or License key to tags');
      continue;
    }

    // Handle specific property keys (line 151-168)
    if (
      cpk === PropertyKeys.RHOAIModelCatalogSourceModelVersion ||
      cpk === PropertyKeys.RHOAIModelCatalogSourceModelKey ||
      cpk === PropertyKeys.RHOAIModelCatalogRegisteredFromKey ||
      cpk === PropertyKeys.RHOAIModelCatalogProviderKey ||
      cpk === PropertyKeys.APITypeKey ||
      cpk === PropertyKeys.RHOAIModelRegistryRegisteredFromCatalogRepositoryName
    ) {
      let v = '';
      if (cpv.metadataStringValue) {
        v = cpv.metadataStringValue.stringValue.toLowerCase();
      }
      if (v.length > 0 && regex.test(v) && v.length <= 63) {
        tags[cpk] = v;
      }
      continue;
    }

    // Handle last modified timestamp (line 169-185)
    if (cpk === PropertyKeys.RHOAIModelRegistryLastModified && lastMod) {
      let v = '';
      if (cpv.metadataStringValue) {
        v = cpv.metadataStringValue.stringValue;
        v = v.replace(/:/g, '-');
        v = v.replace(/\./g, '-');
        v = v.replace(/T/g, '-');
        v = v.replace(/Z/g, '');
        v = `last-modified-time-${v}`;
      }
      if (v.length > 0 && regex.test(v) && v.length <= 63) {
        v = v.toLowerCase();
        tags[cpk] = v;
      }
      continue;
    }

    // Default handling (line 186-194)
    let v = cpk;
    if (
      cpv.metadataStringValue &&
      cpv.metadataStringValue.stringValue.length > 0
    ) {
      v = `${v}-${cpv.metadataStringValue.stringValue.toLowerCase()}`;
    }
    if (v.length > 0 && regex.test(v) && v.length <= 63) {
      tags[cpk] = v;
    }
  }

  return tags;
}

// Helper function: Get string property value
// Converted from commonGetStringPropVal (kfmr.go line 199)
export function getStringPropVal(
  key: string,
  mv: ModelVersion,
  rm: RegisteredModel,
): string | undefined {
  // Check model version custom properties first
  if (mv.customProperties) {
    const mvValue = innerGetStringPropVal(key, mv.customProperties);
    if (mvValue) {
      return mvValue;
    }
  }

  // Check registered model custom properties
  if (rm.customProperties) {
    return innerGetStringPropVal(key, rm.customProperties);
  }

  return undefined;
}

// Helper function: Inner get string property value
// Converted from innerGetStringPropVal (kfmr.go line 218)
function innerGetStringPropVal(
  key: string,
  vmap: { [key: string]: MetadataValue },
): string | undefined {
  const v = vmap[key];
  if (!v) {
    return undefined;
  }

  if (v.metadataStringValue) {
    return v.metadataStringValue.stringValue;
  }

  return undefined;
}

// Sanitize name helper
function sanitizeName(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9-]/g, '-');
}

// Sanitize model version helper
function sanitizeModelVersion(version: string): string {
  // Similar to sanitizeName but may have different rules
  return version.toLowerCase().replace(/[^a-z0-9-]/g, '-');
}

// Main function: Call backstage printers
// Converted from CallBackstagePrinters (kfmr.go line 711)
export async function callBackstagePrinters(
  owner: string,
  lifecycle: string,
  rm: RegisteredModel,
  mv: ModelVersion,
  mas: ModelArtifact[],
  // kfmrIS: KFMRInferenceService | null,
  kserveIS: KServeInferenceService | null,
  format: NormalizerFormat,
): Promise<string> {
  console.log(
    `callBackstagePrinters: format=${format}, rm=${rm.name}, mv=${mv.name}`,
  );

  switch (format) {
    case NormalizerFormat.JsonArrayFormat:
      return generateJsonArrayFormat(
        owner,
        lifecycle,
        rm,
        mv,
        mas,
        /* kfmrIS,*/ kserveIS,
      );
    case NormalizerFormat.CatalogInfoYamlFormat:
    default:
      return generateCatalogInfoYaml(
        owner,
        lifecycle,
        rm,
        mv,
        mas /* kfmrIS, kserveIS*/,
      );
  }
}

// Generate JSON array format output
// Converted from PrintModelCatalogPopulator logic (kfmr.go line 726-732)
function generateJsonArrayFormat(
  owner: string,
  lifecycle: string,
  rm: RegisteredModel,
  mv: ModelVersion,
  mas: ModelArtifact[],
  // kfmrIS: KFMRInferenceService | null,
  kserveIS: KServeInferenceService | null,
): string {
  const model = {
    name: `${sanitizeName(rm.name)}-${sanitizeModelVersion(mv.name)}`,
    owner: getOwner(owner, rm),
    lifecycle: getLifecycle(lifecycle, mv, rm),
    description: `${rm.description || ''}\n${mv.description || ''}`,
    tags: buildTags(rm, mv, mas),
    artifactLocationURL: mas.length > 0 ? mas[0].uri : undefined,
    annotations: {
      'model-name': `${sanitizeName(rm.name)}-${sanitizeModelVersion(mv.name)}`,
    },
  };

  const modelServer = kserveIS
    ? {
        name: sanitizeName(kserveIS.metadata.name),
        owner: getOwner(owner, rm),
        lifecycle: getLifecycle(lifecycle, mv, rm),
        description: `${rm.description || ''}\n${mv.description || ''}`,
        tags: buildTags(rm, mv, mas),
        api: {
          type: 'openapi',
          url: kserveIS.status?.url || kserveIS.status?.address?.url || '',
          spec: 'TBD',
        },
      }
    : null;

  const result = {
    models: [model],
    modelServers: modelServer ? [modelServer] : [],
  };

  return JSON.stringify(result, null, 2);
}

// Generate catalog-info.yaml format output
// Converted from catalog-info.yaml printer logic (kfmr.go line 733-768)
function generateCatalogInfoYaml(
  owner: string,
  lifecycle: string,
  rm: RegisteredModel,
  mv: ModelVersion,
  mas: ModelArtifact[],
  // kfmrIS: KFMRInferenceService | null,
  // kserveIS: KServeInferenceService | null,
): string {
  const yamlParts: string[] = [];

  // Component (kfmr.go line 736)
  yamlParts.push(
    generateComponentYaml(owner, lifecycle, rm, mv, mas /* , kserveIS*/),
  );

  // Resource (kfmr.go line 742-755)
  yamlParts.push(generateResourceYaml(owner, lifecycle, rm, mv, mas));

  // API (kfmr.go line 757-767)
  yamlParts.push(
    generateApiYaml(owner, lifecycle, rm /* mv, kfmrIS, kserveIS*/),
  );

  return yamlParts.join('\n---\n');
}

// Generate Component YAML
function generateComponentYaml(
  owner: string,
  lifecycle: string,
  rm: RegisteredModel,
  mv: ModelVersion,
  mas: ModelArtifact[],
  // kserveIS: KServeInferenceService | null,
): string {
  const tags = buildTags(rm, mv, mas);
  const links = mas.map(ma => ({
    url: ma.uri || '',
    title: ma.description || ma.name || '',
    icon: 'web-asset',
    type: 'website',
  }));

  const component = {
    apiVersion: 'backstage.io/v1alpha1',
    kind: 'Component',
    metadata: {
      name: sanitizeName(rm.name),
      description: rm.description || '',
      tags: tags,
      links: links,
    },
    spec: {
      type: 'model',
      lifecycle: getLifecycle(lifecycle, mv, rm),
      owner: getOwner(owner, rm),
      dependsOn: [`resource:${mv.name}`, ...mas.map(ma => `api:${ma.name}`)],
    },
  };

  return JSON.stringify(component, null, 2);
}

// Generate Resource YAML
function generateResourceYaml(
  owner: string,
  lifecycle: string,
  rm: RegisteredModel,
  mv: ModelVersion,
  mas: ModelArtifact[],
): string {
  const tags = buildTagsForModelVersion(mv, mas);
  const links = mas.map(ma => ({
    url: ma.uri || '',
    title: ma.description || ma.name || '',
    icon: 'web-asset',
    type: 'website',
  }));

  const resource = {
    apiVersion: 'backstage.io/v1alpha1',
    kind: 'Resource',
    metadata: {
      name: mv.name,
      description: mv.description || '',
      tags: tags,
      links: links,
    },
    spec: {
      type: 'model-version',
      lifecycle: getLifecycle(lifecycle, mv, rm),
      owner: getOwner(owner, rm),
      dependencyOf: [`component:${sanitizeName(rm.name)}`],
    },
  };

  return JSON.stringify(resource, null, 2);
}

// Generate API YAML
function generateApiYaml(
  owner: string,
  lifecycle: string,
  rm: RegisteredModel,
  // mv: ModelVersion,
  // kfmrIS: KFMRInferenceService | null,
  // kserveIS: KServeInferenceService | null,
): string {
  const api = {
    apiVersion: 'backstage.io/v1alpha1',
    kind: 'API',
    metadata: {
      name: sanitizeName(rm.name),
      description: rm.description || '',
    },
    spec: {
      type: 'openapi',
      lifecycle: lifecycle,
      owner: getOwner(owner, rm),
      definition: 'no-definition-yet',
      dependencyOf: [`component:${sanitizeName(rm.name)}`],
    },
  };

  return JSON.stringify(api, null, 2);
}

// Helper: Get owner with fallback
function getOwner(defaultOwner: string, rm: RegisteredModel): string {
  if (defaultOwner && defaultOwner.length > 0) {
    return sanitizeName(defaultOwner);
  }
  if (rm.owner) {
    return sanitizeName(rm.owner);
  }
  return sanitizeName(defaultOwner);
}

// Helper: Get lifecycle with fallback
function getLifecycle(
  defaultLifecycle: string,
  mv: ModelVersion,
  rm: RegisteredModel,
): string {
  const lifecycle = getStringPropVal(PropertyKeys.Lifecycle, mv, rm);
  if (lifecycle) {
    return sanitizeName(lifecycle);
  }
  return defaultLifecycle;
}

// Helper: Build tags from registered model, model version, and artifacts
function buildTags(
  rm: RegisteredModel,
  mv: ModelVersion,
  mas: ModelArtifact[],
): string[] {
  const tagsMap: { [key: string]: string } = {};

  // Get tags from registered model
  if (rm.customProperties) {
    const rmTags = getTagsFromCustomProps(false, rm.customProperties);
    Object.assign(tagsMap, rmTags);
  }

  // Get tags from model version
  if (mv.customProperties) {
    const mvTags = getTagsFromCustomProps(true, mv.customProperties);
    Object.assign(tagsMap, mvTags);
  }

  // Get tags from model artifacts
  for (const ma of mas) {
    if (ma.customProperties) {
      const maTags = getTagsFromCustomProps(true, ma.customProperties);
      Object.assign(tagsMap, maTags);
    }
  }

  return Object.values(tagsMap);
}

// Helper: Build tags for model version only
function buildTagsForModelVersion(
  mv: ModelVersion,
  mas: ModelArtifact[],
): string[] {
  const tagsMap: { [key: string]: string } = {};

  // Get tags from model version
  if (mv.customProperties) {
    const mvTags = getTagsFromCustomProps(true, mv.customProperties);
    Object.assign(tagsMap, mvTags);
  }

  // Get tags from model artifacts
  for (const ma of mas) {
    if (ma.customProperties) {
      const maTags = getTagsFromCustomProps(true, ma.customProperties);
      Object.assign(tagsMap, maTags);
    }
  }

  return Object.values(tagsMap);
}

// Export additional helper functions that may be needed
export { sanitizeName, sanitizeModelVersion };
