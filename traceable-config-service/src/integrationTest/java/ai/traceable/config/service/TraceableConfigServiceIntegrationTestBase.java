package ai.traceable.config.service;

import com.typesafe.config.ConfigFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

/** This class starts and shuts down the services required for integration test */
public class TraceableConfigServiceIntegrationTestBase {

  protected static final String SERVICE_NAME = "traceable-config-service";
  protected static final String TENANT_ID = "tenant1";
  private static final String DATA_STORE_COLLECTION = "configurations";
  private static final Collection CONFIGURATIONS_COLLECTION = getConfigurationsCollection();
  protected static ManagedChannel managedChannelForInternalServices;
  protected static ManagedChannel managedChannelForExternalServices;
  private static Server mockActorServer;
  private static Server mockInsightsServer;
  private static Server mockLicenseMeteringServer;
  private static Server mockFeatureFlagServer;

  @BeforeAll
  static void setup() throws IOException {
    System.out.println("Starting Config Service E2E Test");
    IntegrationTestServerUtil.startServices(new String[] {SERVICE_NAME});

    managedChannelForInternalServices =
        ManagedChannelBuilder.forAddress("localhost", 60101).usePlaintext().build();
    managedChannelForExternalServices =
        ManagedChannelBuilder.forAddress("localhost", 60102).usePlaintext().build();

    mockActorServer =
        ServerBuilder.forPort(60888).addService(new MockActorService()).build().start();
    mockInsightsServer =
        ServerBuilder.forPort(60098).addService(new MockInsightsService()).build().start();
    mockLicenseMeteringServer =
        ServerBuilder.forPort(60099)
            .addService(
                ServerInterceptors.intercept(
                    new MockLicenseMeteringService(), new TestInterceptor()))
            .build()
            .start();
    mockFeatureFlagServer =
        ServerBuilder.forPort(60097).addService(new MockFeatureFlagService()).build().start();
  }

  @AfterAll
  static void teardown() {
    managedChannelForInternalServices.shutdown();
    managedChannelForExternalServices.shutdown();
    IntegrationTestServerUtil.shutdownServices();
    mockActorServer.shutdown();
    mockInsightsServer.shutdown();
    mockLicenseMeteringServer.shutdown();
    mockFeatureFlagServer.shutdown();
  }

  private static Collection getConfigurationsCollection() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "localhost");
    configMap.put("port", "37017");
    Datastore datastore =
        DatastoreProvider.getDatastore("mongo", ConfigFactory.parseMap(configMap));
    return datastore.getCollection(DATA_STORE_COLLECTION);
  }

  // Need to delete the collection after each test for stateless integration testing
  @AfterEach
  void delete() {
    CONFIGURATIONS_COLLECTION.deleteAll();
  }
}
