/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * RemoteArtifactRepository provides a remote implementation of ArtifactRepository
 */
public class RemoteArtifactRepository implements ArtifactRepository {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteArtifactRepository.class);

  private final LocationFactory locationFactory;
  private final ArtifactRepositoryReader artifactRepositoryReader;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final RemoteClient remoteClientInternal;
  private final ArtifactLocalizerClient artifactLocalizerClient;

  @VisibleForTesting
  @Inject
  public RemoteArtifactRepository(CConfiguration cConf, ArtifactRepositoryReader artifactRepositoryReader,
                                  ProgramRunnerFactory programRunnerFactory,
                                  RemoteClientFactory remoteClientFactory,
                                  ArtifactLocalizerClient artifactLocalizerClient,
                                  LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, programRunnerFactory);
    this.artifactLocalizerClient = artifactLocalizerClient;
    this.remoteClientInternal = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false),
      String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));
  }

  @Override
  public CloseableClassLoader createArtifactClassLoader(ArtifactDescriptor artifactDescriptor,
                                                        EntityImpersonator entityImpersonator) throws IOException {
    ArtifactId id = new ArtifactId(artifactDescriptor.getNamespace(),
                                   artifactDescriptor.getArtifactId().getName(),
                                   artifactDescriptor.getArtifactId().getVersion().getVersion());
    Location localizedLocation = localizedArtifact(id);
    return artifactClassLoaderFactory.createClassLoader(localizedLocation, entityImpersonator);
  }

  @Override
  public void clear(NamespaceId namespace) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, boolean includeSystem) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ArtifactSummary> getArtifactSummaries(ArtifactRange range, int limit,
                                                    ArtifactSortOrder order) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ApplicationClassSummary> getApplicationClasses(NamespaceId namespace,
                                                             boolean includeSystem) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ApplicationClassInfo> getApplicationClasses(NamespaceId namespace, String className) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>>
  getPlugins(NamespaceId namespace,
             Id.Artifact artifactId) throws IOException, ArtifactNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<ArtifactDescriptor, Set<PluginClass>>
  getPlugins(NamespaceId namespace, Id.Artifact artifactId,
             String pluginType) throws IOException, ArtifactNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<ArtifactDescriptor, PluginClass>
  getPlugins(NamespaceId namespace, Id.Artifact artifactId,
             String pluginType, String pluginName,
             Predicate<ArtifactId> pluginPredicate, int limit,
             ArtifactSortOrder order) throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass>
  findPlugin(NamespaceId namespace, ArtifactRange artifactRange,
             String pluginType, String pluginName,
             PluginSelector selector) throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Map<String, String> properties) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeArtifactProperties(Id.Artifact artifactId, Map<String, String> properties) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeArtifactProperty(Id.Artifact artifactId, String key, String value) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteArtifactProperty(Id.Artifact artifactId, String key) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteArtifactProperties(Id.Artifact artifactId) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSystemArtifacts() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ArtifactInfo> getArtifactsInfo(NamespaceId namespace) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    return artifactRepositoryReader.getArtifact(artifactId);
  }

  @Override
  public InputStream newInputStream(Id.Artifact artifactId) throws IOException, NotFoundException {
    return artifactRepositoryReader.newInputStream(artifactId);
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit,
                                                 ArtifactSortOrder order) throws Exception {
    return artifactRepositoryReader.getArtifactDetails(range, limit, order);
  }

  private Location localizedArtifact(ArtifactId artifactId) throws IOException {
    LOG.debug("wyzhang: localizeArtifact: {}", artifactId);

    try {
      return Locations.toLocation(artifactLocalizerClient.getArtifactLocation(artifactId));
    } catch (ArtifactNotFoundException e) {
      throw new IOException(String.format("Artifact %s is not found", artifactId), e);
    }
  }
}
