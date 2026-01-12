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

// Converted from kserve.go in model-catalog-bridge

import { NormalizerFormat, PropertyKeys } from './Kfmr';

// Annotation prefix (from brdgtypes package)
const ANNOTATION_PREFIX = 'model-catalog-bridge.ai.redhat.com/';

// Model framework constants (from kserve.go line 25-36)
const FRAMEWORK_SKLEARN = 'sklearn';
const FRAMEWORK_XGBOOST = 'xgboost';
const FRAMEWORK_TENSORFLOW = 'tensorflow';
const FRAMEWORK_PYTORCH = 'pytorch';
const FRAMEWORK_TRITON = 'triton';
const FRAMEWORK_ONNX = 'onnx';
const FRAMEWORK_HUGGINGFACE = 'huggingface';
const FRAMEWORK_PMML = 'pmml';
const FRAMEWORK_LIGHTGBM = 'lightgbm';
const FRAMEWORK_PADDLE = 'paddle';

// InferenceService interface (matching KServe API)
export interface KServeInferenceService {
  metadata: {
    name: string;
    namespace: string;
    labels?: { [key: string]: string };
    annotations?: { [key: string]: string };
  };
  spec: {
    predictor: {
      sklearn?: any;
      xgboost?: any;
      tensorflow?: any;
      pytorch?: any;
      triton?: any;
      onnx?: any;
      huggingface?: any;
      pmml?: any;
      lightgbm?: any;
      paddle?: any;
      model?: {
        modelFormat: {
          name: string;
          version?: string;
        };
        storageURI?: string;
        storage?: {
          path?: string;
        };
      };
    };
    explainer?: {
      art?: {
        type: string;
      };
    };
  };
  status?: {
    url?: {
      toString(): string;
    };
    address?: {
      url: string;
    };
    components?: {
      [key: string]: {
        url?: {
          toString(): string;
        };
        restURL?: {
          toString(): string;
        };
        grpcURL?: {
          toString(): string;
        };
      };
    };
  };
}

// Helper function: Sanitize name
function sanitizeName(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9-]/g, '-');
}

// Helper function: Fix key for annotation (kserve.go line 315)
function fixKeyForAnnotation(key: string): string {
  const lowerKey = key.toLowerCase();
  return lowerKey.replace(/ /g, '');
}

// Helper function: Get string property value from annotations (kserve.go line 322)
function getStringPropVal(
  key: string,
  is: KServeInferenceService,
): string | undefined {
  if (!is || !is.metadata.annotations) {
    return undefined;
  }

  const annotationKey = `${ANNOTATION_PREFIX}${fixKeyForAnnotation(key)}`;
  const val = is.metadata.annotations[annotationKey];

  return val || undefined;
}

// Get name (namespace_name format) - kserve.go line 54
function getName(is: KServeInferenceService): string {
  return `${is.metadata.namespace}_${is.metadata.name}`;
}

// Get description - kserve.go line 60
function getDescription(is: KServeInferenceService): string {
  return `KServe instance ${is.metadata.namespace}:${is.metadata.name}`;
}

// Get links from InferenceService - kserve.go line 64
function getLinks(is: KServeInferenceService): Array<{
  url: string;
  title: string;
  type: string;
  icon: string;
}> {
  const links: Array<{
    url: string;
    title: string;
    type: string;
    icon: string;
  }> = [];

  if (!is) {
    return links;
  }

  // Main URL
  if (is.status?.url) {
    links.push({
      url: is.status.url.toString(),
      title: 'API URL',
      type: 'website',
      icon: 'web-asset',
    });
  }

  // Component URLs
  if (is.status?.components) {
    for (const [componentType, componentStatus] of Object.entries(
      is.status.components,
    )) {
      if (componentStatus.url) {
        links.push({
          url: `${componentStatus.url.toString()}/docs`,
          title: `${componentType} FastAPI URL`,
          icon: 'web-asset',
          type: 'website',
        });
        links.push({
          url: componentStatus.url.toString(),
          title: `${componentType} model serving URL`,
          icon: 'web-asset',
          type: 'website',
        });
      }
      if (componentStatus.restURL) {
        links.push({
          url: componentStatus.restURL.toString(),
          title: `${componentType} REST model serving URL`,
          icon: 'web-asset',
          type: 'website',
        });
      }
      if (componentStatus.grpcURL) {
        links.push({
          url: componentStatus.grpcURL.toString(),
          title: `${componentType} GRPC model serving URL`,
          icon: 'web-asset',
          type: 'website',
        });
      }
    }
  }

  return links;
}

// Get tags from predictor spec - kserve.go line 113
function getTags(is: KServeInferenceService): string[] {
  const tags: string[] = [];

  if (!is) {
    return tags;
  }

  const predictor = is.spec.predictor;

  // Check predictor types (Go uses fallthrough, so we check all)
  if (predictor.sklearn) tags.push(FRAMEWORK_SKLEARN);
  if (predictor.xgboost) tags.push(FRAMEWORK_XGBOOST);
  if (predictor.tensorflow) tags.push(FRAMEWORK_TENSORFLOW);
  if (predictor.pytorch) tags.push(FRAMEWORK_PYTORCH);
  if (predictor.triton) tags.push(FRAMEWORK_TRITON);
  if (predictor.onnx) tags.push(FRAMEWORK_ONNX);
  if (predictor.huggingface) tags.push(FRAMEWORK_HUGGINGFACE);
  if (predictor.pmml) tags.push(FRAMEWORK_PMML);
  if (predictor.lightgbm) tags.push(FRAMEWORK_LIGHTGBM);
  if (predictor.paddle) tags.push(FRAMEWORK_PADDLE);

  // Generic model format
  if (predictor.model) {
    const modelFormat = predictor.model.modelFormat;
    let tag = modelFormat.name;
    if (modelFormat.version) {
      tag = `${tag}-${modelFormat.version}`;
    }
    tags.push(tag.toLowerCase());
  }

  // Explainer
  if (is.spec.explainer?.art) {
    tags.push(is.spec.explainer.art.type.toLowerCase());
  }

  return tags;
}

// Get tags from labels - used for ModelServer and API - kserve.go line 391
function getTagsFromLabels(is: KServeInferenceService): string[] {
  const tags: string[] = [];

  if (!is.metadata.labels) {
    return tags;
  }

  for (const [k, v] of Object.entries(is.metadata.labels)) {
    const tag = `${sanitizeName(k)}-${sanitizeName(v)}`;
    tags.push(sanitizeName(tag));
  }

  return tags;
}

// Get artifact location URL - kserve.go line 580
function getArtifactLocationURL(
  is: KServeInferenceService,
): string | undefined {
  const model = is.spec.predictor.model;

  if (model?.storageURI) {
    return model.storageURI;
  }

  if (model?.storage?.path) {
    return `s3://${model.storage.path}`;
  }

  return undefined;
}

// Main function: Call backstage printers for KServe
// Converted from CallBackstagePrinters (kserve.go line 260)
export async function callBackstagePrinters(
  owner: string,
  lifecycle: string,
  is: KServeInferenceService,
  format: NormalizerFormat,
): Promise<string> {
  console.log(
    `KServe.callBackstagePrinters: format=${format}, namespace=${is.metadata.namespace}, name=${is.metadata.name}`,
  );

  switch (format) {
    case NormalizerFormat.JsonArrayFormat:
      return generateJsonArrayFormat(owner, lifecycle, is);
    case NormalizerFormat.CatalogInfoYamlFormat:
    default:
      return generateCatalogInfoYaml(owner, lifecycle, is);
  }
}

// Generate JSON array format output (kserve.go line 269-276)
function generateJsonArrayFormat(
  owner: string,
  lifecycle: string,
  is: KServeInferenceService,
): string {
  const name = `${sanitizeName(is.metadata.namespace)}-${sanitizeName(
    is.metadata.name,
  )}`;

  // Get property values with fallbacks
  const ownerValue =
    getStringPropVal(PropertyKeys.Owner, is) || sanitizeName(owner);
  const lifecycleValue =
    getStringPropVal(PropertyKeys.Lifecycle, is) || lifecycle;
  const description = getStringPropVal(PropertyKeys.DescriptionKey, is) || '';
  const techdocsUrl = getStringPropVal(PropertyKeys.TechDocsKey, is);

  // Build model object (kserve.go line 646-674)
  const model = {
    name: name,
    owner: sanitizeName(ownerValue),
    lifecycle: lifecycleValue,
    description: description,
    tags: getTags(is),
    artifactLocationURL: getArtifactLocationURL(is),
    ethics: getStringPropVal(PropertyKeys.EthicsKey, is),
    howToUseURL: getStringPropVal(PropertyKeys.HowToUseKey, is),
    support: getStringPropVal(PropertyKeys.SupportKey, is),
    training: getStringPropVal(PropertyKeys.TrainingKey, is),
    usage: getStringPropVal(PropertyKeys.UsageKey, is),
    license: getStringPropVal(PropertyKeys.LicenseKey, is),
    annotations: {
      'model-name': is.metadata.name,
      ...(techdocsUrl ? { [PropertyKeys.TechDocsKey]: techdocsUrl } : {}),
    },
  };

  // Build model server object (kserve.go line 679-698)
  const modelServer = {
    name: sanitizeName(name),
    owner: sanitizeName(ownerValue),
    lifecycle: lifecycleValue,
    description: description,
    homepageURL: getStringPropVal(PropertyKeys.HomepageURLKey, is),
    usage: getStringPropVal(PropertyKeys.UsageKey, is),
    tags: getTagsFromLabels(is),
    authentication: false, // TODO: Implement service account check
    api: {
      type: getStringPropVal(PropertyKeys.APITypeKey, is) || 'openapi',
      spec: getStringPropVal(PropertyKeys.APISpecKey, is) || 'TBD',
      tags: getTagsFromLabels(is),
      url: is.status?.url?.toString() || is.status?.address?.url || '',
      annotations: {},
    },
    annotations: {},
  };

  const result = {
    models: [model],
    modelServers: [modelServer],
  };

  return JSON.stringify(result, null, 2);
}

// Generate catalog-info.yaml format output (kserve.go line 278-304)
function generateCatalogInfoYaml(
  owner: string,
  lifecycle: string,
  is: KServeInferenceService,
): string {
  const yamlParts: string[] = [];

  // Component (kserve.go line 280)
  yamlParts.push(generateComponentYaml(owner, lifecycle, is));

  // Resource (kserve.go line 285-293)
  yamlParts.push(generateResourceYaml(owner, lifecycle, is));

  // API (kserve.go line 296-302)
  yamlParts.push(generateApiYaml(owner, lifecycle, is));

  return yamlParts.join('\n---\n');
}

// Generate Component YAML
function generateComponentYaml(
  owner: string,
  lifecycle: string,
  is: KServeInferenceService,
): string {
  const name = getName(is);

  const component = {
    apiVersion: 'backstage.io/v1alpha1',
    kind: 'Component',
    metadata: {
      name: name,
      description: getDescription(is),
      tags: getTags(is),
      links: getLinks(is),
    },
    spec: {
      type: 'model',
      lifecycle: lifecycle,
      owner: owner,
      dependsOn: [`resource:${name}`, `api:${name}`],
      providesApis: [name],
    },
  };

  return JSON.stringify(component, null, 2);
}

// Generate Resource YAML
function generateResourceYaml(
  owner: string,
  lifecycle: string,
  is: KServeInferenceService,
): string {
  const name = getName(is);

  const resource = {
    apiVersion: 'backstage.io/v1alpha1',
    kind: 'Resource',
    metadata: {
      name: name,
      description: getDescription(is),
    },
    spec: {
      type: 'model-version',
      lifecycle: lifecycle,
      owner: owner,
      dependencyOf: [`component:${name}`],
    },
  };

  return JSON.stringify(resource, null, 2);
}

// Generate API YAML
function generateApiYaml(
  owner: string,
  lifecycle: string,
  is: KServeInferenceService,
): string {
  const name = getName(is);

  const api = {
    apiVersion: 'backstage.io/v1alpha1',
    kind: 'API',
    metadata: {
      name: name,
      description: getDescription(is),
    },
    spec: {
      type: 'openapi',
      lifecycle: lifecycle,
      owner: owner,
      definition: '', // Would fetch from /openapi.json endpoint
      dependencyOf: [`component:${name}`],
    },
  };

  return JSON.stringify(api, null, 2);
}

// Export helper functions that may be needed
export { getName, getDescription, getTags, getLinks };
