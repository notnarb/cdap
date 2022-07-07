/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.k8s.runtime;

import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentList;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetList;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watchable;
import okhttp3.Call;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A thread for monitoring Kubernetes application resource state change.
 *
 * @param <T> type of Kubernetes resource for which state changes to be monitored
 */
abstract class AppResourceWatcherThread<T> extends AbstractWatcherThread<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AppResourceWatcherThread.class);

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1Deployment} events.
   */
  static AppResourceWatcherThread<V1Deployment> createDeploymentWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1Deployment>("apps", "v1", "deployments", namespace, selector) { };
  }

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1StatefulSet} events.
   */
  static AppResourceWatcherThread<V1StatefulSet> createStatefulSetWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1StatefulSet>("apps", "v1", "statefulsets", namespace, selector) { };
  }

  /**
   * Creates a {@link AppResourceWatcherThread} for watching {@link V1Job} events.
   */
  static AppResourceWatcherThread<V1Job> createJobWatcher(String namespace, String selector) {
    return new AppResourceWatcherThread<V1Job>("batch", "v1", "jobs", namespace, selector) { };
  }

  private final String group;
  private final String version;
  private final String plural;
  private final String selector;
  private final Queue<ResourceChangeListener<T>> listeners;
  private volatile ApiClient apiClient;
  private String resourceVersion;

  private AppResourceWatcherThread(String group, String version, String plural, String namespace, String selector) {
    super("kube-" + plural + "-watch", namespace);
    setDaemon(true);
    this.group = group;
    this.version = version;
    this.plural = plural;
    this.selector = selector;
    this.listeners = new ConcurrentLinkedQueue<>();
  }

  Cancellable addListener(ResourceChangeListener<T> listener) {
    // Wrap the listener for removal
    ResourceChangeListener<T> wrappedListener = wrapListener(listener);
    listeners.add(wrappedListener);
    resetWatch();
    return () -> listeners.remove(wrappedListener);
  }

  @Override
  protected Watchable<T> createWatchable(Type resourceType, String namespace,
                                         @Nullable String labelSelector) throws IOException, ApiException {
    ApiClient apiClient = getApiClient();
    Call call;
    if (resourceType.equals(V1Job.class)) {
      call = getCallForJobList(namespace, labelSelector);
    } else if (resourceType.equals(V1StatefulSet.class)) {
      call = getCallForStatefulSetList(namespace, labelSelector);
    } else if (resourceType.equals(V1Deployment.class)) {
      call = getCallForDeploymentList(namespace, labelSelector);
    } else {
      CustomObjectsApi api = new CustomObjectsApi(apiClient);
      call = api.listNamespacedCustomObjectCall(group, version, namespace, plural, null, null, null,
                                                labelSelector, null, null, null, true, null);
    }
    return Watch.createWatch(apiClient, call, TypeToken.getParameterized(Watch.Response.class,
                                                                         resourceType).getType());
  }

  private Call getCallForJobList(String namespace, @Nullable String labelSelector) throws IOException, ApiException {
    ApiClient apiClient = getApiClient();
    BatchV1Api api = new BatchV1Api(apiClient);
    if (resourceVersion == null) {
      V1JobList jobList = api.listNamespacedJob(namespace, null, null, null, null, labelSelector,
                                                       null, null, null, null, null);
      resourceVersion = jobList.getMetadata().getResourceVersion();
    }
    return api.listNamespacedJobCall(namespace, null, null, null, null, labelSelector,
                                            null, resourceVersion, null, null, true, null);
  }

  private Call getCallForStatefulSetList(String namespace, @Nullable String labelSelector)
    throws IOException, ApiException {
    ApiClient apiClient = getApiClient();
    AppsV1Api api = new AppsV1Api(apiClient);
    if (resourceVersion == null) {
      V1StatefulSetList stateFulSetList =  api.listNamespacedStatefulSet(namespace, null, null, null,
                                                                         null, labelSelector, null, null,
                                                                         null, null, null);
      resourceVersion = stateFulSetList.getMetadata().getResourceVersion();
    }
    return api.listNamespacedStatefulSetCall(namespace, null, null, null, null, labelSelector,
                                             null, resourceVersion, null, null, true, null);
  }

  private Call getCallForDeploymentList(String namespace, @Nullable String labelSelector)
    throws IOException, ApiException {
    ApiClient apiClient = getApiClient();
    AppsV1Api api = new AppsV1Api(apiClient);
    if (resourceVersion == null) {
      V1DeploymentList stateFulSetList =  api.listNamespacedDeployment(namespace, null, null, null,
                                                                       null, labelSelector, null, null,
                                                                       null, null, null);
      resourceVersion = stateFulSetList.getMetadata().getResourceVersion();
    }
    return api.listNamespacedDeploymentCall(namespace, null, null, null, null, labelSelector,
                                            null, resourceVersion, null, null, true, null);
  }

  @Nullable
  @Override
  protected String getSelector() {
    return selector;
  }

  @Override
  public void resourceAdded(T resource) {
    listeners.forEach(l -> l.resourceAdded(resource));
  }

  @Override
  public void resourceModified(T resource) {
    listeners.forEach(l -> l.resourceModified(resource));
  }

  @Override
  public void resourceDeleted(T resource) {
    listeners.forEach(l -> l.resourceDeleted(resource));
  }

  @Override
  public void watchError(V1Status status) {
    LOG.trace("Got an error on watch for plural {}, status: {}", plural, status);
    // This happens when the specified resource version is too old, so we reset the resource version and restart
    // the watch.
    resourceVersion = null;
    listeners.forEach(l -> l.watchError(status));
  }

  private ApiClient getApiClient() throws IOException {
    ApiClient client = apiClient;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = apiClient;
      if (client != null) {
        return client;
      }

      client = Config.defaultClient();

      // Set a reasonable timeout for the watch.
      client.setReadTimeout((int) TimeUnit.MINUTES.toMillis(5));
      apiClient = client;
      return client;
    }
  }

  private ResourceChangeListener<T> wrapListener(ResourceChangeListener<T> listener) {
    return new ResourceChangeListener<T>() {

      @Override
      public void resourceAdded(T resource) {
        listener.resourceAdded(resource);
      }

      @Override
      public void resourceModified(T resource) {
        listener.resourceModified(resource);
      }

      @Override
      public void resourceDeleted(T resource) {
        listener.resourceDeleted(resource);
      }

      @Override
      public void watchError(V1Status status) {
        listener.watchError(status);
      }
    };
  }
}
