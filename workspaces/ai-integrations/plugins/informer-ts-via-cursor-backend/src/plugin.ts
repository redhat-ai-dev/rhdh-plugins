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
  coreServices,
  createBackendPlugin,
} from '@backstage/backend-plugin-api';
import { createRouter } from './router';
import { catalogServiceRef } from '@backstage/plugin-catalog-node/alpha';
import { createTodoListService } from './services/TodoListService';
import { InferenceServiceController } from './controller/InferenceServiceController';

/**
 * tsControllerCursorPlugin backend plugin
 *
 * @public
 */
export const tsControllerCursorPlugin = createBackendPlugin({
  pluginId: 'ts-controller-cursor',
  register(env) {
    env.registerInit({
      deps: {
        logger: coreServices.logger,
        auth: coreServices.auth,
        httpAuth: coreServices.httpAuth,
        httpRouter: coreServices.httpRouter,
        catalog: catalogServiceRef,
      },
      async init({ logger, auth, httpAuth, httpRouter, catalog }) {
        const todoListService = await createTodoListService({
          logger,
          auth,
          catalog,
        });

        // Initialize and start the InferenceServiceController
        const inferenceServiceController = new InferenceServiceController({
          verbose: true, // Enable verbose logging
        });

        try {
          await inferenceServiceController.start();
          logger.info('InferenceServiceController started successfully');
        } catch (error) {
          logger.error('Failed to start InferenceServiceController:', error);
        }

        httpRouter.use(
          await createRouter({
            httpAuth,
            todoListService,
          }),
        );
      },
    });
  },
});
