/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.securestore.gcp.cloudsecretmanager;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Proof of concept integration test which makes requests to the real GCP Secret Manager using
 * CloudSecretManagerClient.
 *
 * <p>Requires the executor has Application Default Credentials configured in order to access GCP
 * and assumes Secret Manager Admin permissions.
 */
public class CloudSecretManagerClientIntegrationTest {
  private static final String NAMESPACE = "unit_test_namespace";
  private static final String SECRET_NAME = "bhorsley_unit_test";
  private static final String LONG_SECRET_NAME = "bhorsley_unit_test".repeat(20);
  private static final String LONG_NAMESPACE_NAME = "unit_test_namespace".repeat(20);
  private CloudSecretManagerClient client;

  @Before
  public void setUp() throws Exception {
    client = new CloudSecretManagerClient(ImmutableMap.of());

    cleanupSecret(NAMESPACE, SECRET_NAME);
    cleanupSecret(LONG_NAMESPACE_NAME, LONG_SECRET_NAME);
  }

  @Test
  public void createSecret_success() throws Exception {
    String testKey1 = "very_long_key_over_63_characters_long".repeat(10);
    String testKey2 = "key with invalid characters 😳";
    WrappedSecret secret =
        WrappedSecret.fromMetadata(
            NAMESPACE,
            new SecretMetadata(
                SECRET_NAME,
                "initial_description",
                0,
                ImmutableMap.of(testKey1, "value1", testKey2, "value2")));

    client.createSecret(secret);
    SecretMetadata returnedMetadata =
        client.getSecret(NAMESPACE, SECRET_NAME).getCdapSecretMetadata();

    assertEquals("initial_description", returnedMetadata.getDescription());
    // Assert that the creation time is within 60 seconds of the system time -- may fail if local
    // time of sync.
    assertEquals(Instant.now().toEpochMilli(), returnedMetadata.getCreationTimeMs(), 60000);
    assertEquals("value1", returnedMetadata.getProperties().get(testKey1));
    assertEquals("value2", returnedMetadata.getProperties().get(testKey2));
    assertEquals(2, returnedMetadata.getProperties().size());
  }

  @Test
  public void createSecret_longName() throws Exception {
    assertTrue(LONG_SECRET_NAME.length() > 255);
    assertTrue(LONG_NAMESPACE_NAME.length() > 255);
    WrappedSecret secret =
        WrappedSecret.fromMetadata(
            LONG_NAMESPACE_NAME,
            new SecretMetadata(LONG_SECRET_NAME, "description", 0, ImmutableMap.of()));

    client.createSecret(secret);

    WrappedSecret returnedSecret = client.getSecret(LONG_NAMESPACE_NAME, LONG_SECRET_NAME);

    assertEquals(LONG_SECRET_NAME, returnedSecret.getCdapSecretMetadata().getName());
    assertEquals(LONG_NAMESPACE_NAME, returnedSecret.getNamespace());
  }

  @Test
  public void updateSecret_success() throws Exception {
    String updatedDescription = "Updated Description%_😊";
    SecretMetadata initialMetadata =
        new SecretMetadata(
            SECRET_NAME,
            "initial_description",
            0,
            ImmutableMap.of("test_prop", "initial_property"));

    SecretMetadata updatedMetadata =
        new SecretMetadata(
            /* name= */ SECRET_NAME,
            /* description= */ updatedDescription,
            /* creationTimeMs= */ 0,
            /* properties= */ ImmutableMap.of("test_prop", "updated_property"));

    client.createSecret(WrappedSecret.fromMetadata(NAMESPACE, initialMetadata));

    try {
      client.updateSecret(WrappedSecret.fromMetadata(NAMESPACE, updatedMetadata));
    } catch (ApiException e) {
      System.out.println(e.getErrorDetails());
      throw e;
    }

    SecretMetadata fetchedMetadata =
        client.getSecret(NAMESPACE, SECRET_NAME).getCdapSecretMetadata();
    assertEquals(updatedDescription, fetchedMetadata.getDescription());
    assertEquals(ImmutableMap.of("test_prop", "updated_property"), fetchedMetadata.getProperties());
  }

  @Test
  public void accessSecret_success() throws Exception {
    WrappedSecret secret =
        WrappedSecret.fromMetadata(
            NAMESPACE, new SecretMetadata(SECRET_NAME, "desc", 0, ImmutableMap.of()));

    client.createSecret(secret);
    client.addSecretVersion(secret, "testFoo".getBytes());

    assertArrayEquals("testFoo".getBytes(), client.getSecretData(secret));
  }


  private void cleanupSecret(String namespace, String name) throws InvalidSecretException {
    try {
      client.getSecret(namespace, name);
      client.deleteSecret(namespace, name);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
        // Secret already doesn't exist.
        return;
      }
      throw e;
    }
  }
}
