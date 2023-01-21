package org.hypertrace.config.service;

import static org.hypertrace.config.service.IntegrationTestUtils.getConfigValue;

import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import io.grpc.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

public class ConfigServiceIntegrationTestBase {

  protected static final String RESOURCE_NAME_1 = "foo1";
  protected static final String RESOURCE_NAME_2 = "foo2";
  protected static final String RESOURCE_NAMESPACE_1 = "bar1";
  protected static final String RESOURCE_NAMESPACE_2 = "bar2";
  protected static final String TENANT_1 = "tenant1";
  protected static final String TENANT_2 = "tenant2";
  protected static final String CONTEXT_1 = "ctx1";
  protected static final String CONTEXT_2 = "ctx2";
  protected static final String DATA_STORE_COLLECTION = "configurations";
  protected static final Collection CONFIGURATIONS_COLLECTION = getConfigurationsCollection();

  protected static ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  protected static Value config1;
  protected static Value config2;
  protected static Value config3;

  protected static ManagedChannel globalConfigInternalChannel;

  @BeforeAll
  public static void setup() throws IOException {
    System.out.println("Starting Config Service E2E Test");
    IntegrationTestServerUtil.startServices(new String[] {"config-service"});

    Channel channel = ManagedChannelBuilder.forAddress("localhost", 50101).usePlaintext().build();
    globalConfigInternalChannel =
        ManagedChannelBuilder.forAddress("localhost", 50103).usePlaintext().build();

    configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    config1 = getConfigValue("config1.yaml");
    config2 = getConfigValue("config2.yaml");
    config3 = getConfigValue("config3.yaml");
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
    globalConfigInternalChannel.shutdown();
  }

  // Need to delete the collection after each test for stateless integration testing
  @AfterEach
  void delete() {
    CONFIGURATIONS_COLLECTION.deleteAll();
  }

  private static Collection getConfigurationsCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(DATA_STORE_COLLECTION);
  }
}
