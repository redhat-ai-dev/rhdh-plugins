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
import * as yaml from 'js-yaml';

/**
 * KServe InferenceService resource from serving.kserve.io/v1beta1 API
 */
export interface InferenceService {
  apiVersion: 'serving.kserve.io/v1beta1';
  kind: 'InferenceService';
  metadata: {
    name: string;
    namespace?: string;
    resourceVersion?: string;
    uid?: string;
    creationTimestamp?: Date;
    [key: string]: any;
  };
  spec?: any;
  status?: any;
}

/**
 * Configuration for the InferenceService controller
 */
export interface InferenceServiceControllerConfig {
  /** Namespace to watch (optional, defaults to all namespaces) */
  namespace?: string;
  /** Resync period for the informer in milliseconds */
  resyncPeriod?: number;
  /** Whether to log detailed information */
  verbose?: boolean;
}

/**
 * Controller for watching KServe InferenceService resources
 */
export class InferenceServiceController {
  private kc: any;
  private k8sApi: any;
  private config: InferenceServiceControllerConfig;
  private informer: any | null = null;
  private isRunning = false;
  private k8s: any = null;

  constructor(config: InferenceServiceControllerConfig = {}) {
    this.config = {
      resyncPeriod: 30000, // 30 seconds default
      verbose: false,
      ...config,
    };
  }

  /**
   * Dynamically imports the Kubernetes client
   */
  private async loadKubernetesClient(): Promise<void> {
    if (!this.k8s) {
      this.k8s = await import('@kubernetes/client-node');
    }
  }

  /**
   * Loads Kubernetes configuration from default locations
   */
  private async loadKubeConfig(): Promise<void> {
    await this.loadKubernetesClient();

    this.kc = new this.k8s.KubeConfig();
    try {
      // Try to load from default config file first (for local development)
      this.kc.loadFromDefault();
      console.log('✅ Loaded Kubernetes configuration from default location');
    } catch {
      try {
        // Fallback to in-cluster config (when running in a pod)
        this.kc.loadFromCluster();
        console.log('✅ Loaded in-cluster Kubernetes configuration');
      } catch (error) {
        console.error('❌ Failed to load Kubernetes configuration:', error);
        throw new Error('Unable to load Kubernetes configuration');
      }
    }

    // Create the custom objects API client
    this.k8sApi = this.kc.makeApiClient(this.k8s.CustomObjectsApi);
  }

  /**
   * Creates the informer for watching InferenceService resources
   */
  private createInformer(): any {
    const listFunction = () => {
      if (this.config.namespace) {
        // Watch specific namespace
        return this.k8sApi.listNamespacedCustomObject({
          group: 'serving.kserve.io',
          version: 'v1beta1',
          namespace: this.config.namespace,
          plural: 'inferenceservices',
        });
      }
      // Watch all namespaces
      return this.k8sApi.listClusterCustomObject({
        group: 'serving.kserve.io',
        version: 'v1beta1',
        plural: 'inferenceservices',
      });
    };

    const path = this.config.namespace
      ? `/apis/serving.kserve.io/v1beta1/namespaces/${this.config.namespace}/inferenceservices`
      : '/apis/serving.kserve.io/v1beta1/inferenceservices';

    const informer = this.k8s.makeInformer(
      this.kc,
      path,
      listFunction,
      undefined, // No label selector
    );

    return informer;
  }

  /**
   * Outputs the InferenceService resource as YAML
   */
  private outputResourceYaml(obj: any, eventType: string): void {
    if (!this.validateInferenceService(obj)) {
      console.warn('⚠️  Received invalid InferenceService object:', obj);
      return;
    }

    const timestamp = new Date().toISOString();

    console.log(`\n=== InferenceService ${eventType} ===`);
    console.log(`Timestamp: ${timestamp}`);
    console.log(`Name: ${obj.metadata.name}`);
    console.log(`Namespace: ${obj.metadata.namespace || 'default'}`);
    console.log(
      `Resource Version: ${obj.metadata.resourceVersion || 'unknown'}`,
    );
    console.log('\n--- YAML Content ---');

    try {
      // Convert to YAML and output
      const yamlContent = yaml.dump(obj, {
        indent: 2,
        lineWidth: 120,
        noRefs: true,
      });
      console.log(yamlContent);
    } catch (error) {
      console.error('❌ Failed to convert to YAML:', error);
      console.log('Raw object:', JSON.stringify(obj, null, 2));
    }

    console.log('--- End YAML ---\n');
  }

  /**
   * Validates that an InferenceService resource has the expected structure
   */
  private validateInferenceService(obj: any): obj is InferenceService {
    return (
      obj &&
      typeof obj === 'object' &&
      obj.apiVersion === 'serving.kserve.io/v1beta1' &&
      obj.kind === 'InferenceService' &&
      obj.metadata &&
      typeof obj.metadata.name === 'string'
    );
  }

  /**
   * Handles InferenceService resource addition events
   */
  private onAdd = (obj: any): void => {
    this.outputResourceYaml(obj, 'ADDED');
  };

  /**
   * Handles InferenceService resource update events
   */
  private onUpdate = (obj: any): void => {
    this.outputResourceYaml(obj, 'UPDATED');
  };

  /**
   * Handles InferenceService resource deletion events
   */
  private onDelete = (obj: any): void => {
    this.outputResourceYaml(obj, 'DELETED');
  };

  /**
   * Handles informer errors
   */
  private onError = (err: any): void => {
    console.error('❌ InferenceService Informer error:', err);
  };

  /**
   * Starts the InferenceService controller
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      console.warn('⚠️  InferenceService Controller is already running');
      return;
    }

    try {
      console.log('🚀 Starting InferenceService Controller...');

      // Load Kubernetes configuration and client
      await this.loadKubeConfig();

      // Create the informer
      this.informer = this.createInformer();

      // Set up event handlers
      this.informer.on('add', this.onAdd);
      this.informer.on('update', this.onUpdate);
      this.informer.on('delete', this.onDelete);
      this.informer.on('error', this.onError);

      // Start the informer
      await this.informer.start();

      this.isRunning = true;
      console.log('✅ InferenceService Controller started successfully');

      if (this.config.namespace) {
        console.log(
          `🔍 Watching InferenceServices in namespace: ${this.config.namespace}`,
        );
      } else {
        console.log('🔍 Watching InferenceServices in all namespaces');
      }
    } catch (error) {
      console.error('❌ Failed to start InferenceService Controller:', error);
      throw error;
    }
  }

  /**
   * Stops the InferenceService controller
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      console.warn('⚠️  InferenceService Controller is not running');
      return;
    }

    try {
      console.log('🛑 Stopping InferenceService Controller...');

      if (this.informer) {
        await this.informer.stop();
        this.informer = null;
      }

      this.isRunning = false;
      console.log('✅ InferenceService Controller stopped successfully');
    } catch (error) {
      console.error('❌ Error stopping InferenceService Controller:', error);
      throw error;
    }
  }

  /**
   * Gets the current status of the controller
   */
  getStatus(): {
    isRunning: boolean;
    config: InferenceServiceControllerConfig;
  } {
    return {
      isRunning: this.isRunning,
      config: { ...this.config },
    };
  }
}
