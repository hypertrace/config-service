package ai.traceable.config.service;

import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_OBFUSCATE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.sensitivedata.config.service.v1.Filter;
import ai.traceable.sensitivedata.config.service.v1.GetParametersRequest;
import ai.traceable.sensitivedata.config.service.v1.GetParametersResponse;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.MarkParametersRequest;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.ParameterWithRedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.ParameterWithSensitivity;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.IntegrationTestConfigClientFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for {@link TraceableConfigService} */
public class TraceableConfigServiceIntegrationTest {

  private static final String SERVICE_NAME = "traceable-config-service";
  private static final String DEFAULT_PII_FILTER_CONFIG =
      "sensitive.data.config.service.default.pii.filter.config";

  private static SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceStub;
  private static PiiFilterConfigServiceBlockingStub piiFilterConfigServiceStub;

  @BeforeAll
  public static void setup() {
    System.out.println("Starting Config Service E2E Test");
    IntegrationTestServerUtil.startServices(new String[] {SERVICE_NAME});

    ManagedChannel managedChannel =
        ManagedChannelBuilder.forAddress("localhost", 50101).usePlaintext().build();

    sensitiveDataConfigServiceStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(managedChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    piiFilterConfigServiceStub =
        PiiFilterConfigServiceGrpc.newBlockingStub(managedChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
  }

  @Test
  void testSensitiveDataConfiguration() {
    Parameter parameter1 = getParameter(ParamType.PARAM_TYPE_BODY, "p1");
    Parameter parameter2 = getParameter(ParamType.PARAM_TYPE_BODY, "p2");
    Parameter parameter3 = getParameter(ParamType.PARAM_TYPE_QUERY, "authorization");
    Parameter parameter4 = getParameter(ParamType.PARAM_TYPE_HEADER, "access_token");
    String endpoint1 = "/checkout";
    String endpoint2 = "/orders";

    // mark the above parameters as sensitive in context of an endpoint
    markSensitive(List.of(parameter1, parameter2, parameter3), endpoint1, true);
    markSensitive(List.of(parameter1, parameter4), endpoint2, false);
    assertEquals(
        Set.of(parameter1, parameter2),
        getSensitiveParameters(ParamType.PARAM_TYPE_BODY, endpoint1));
    assertEquals(Set.of(parameter3), getSensitiveParameters(ParamType.PARAM_TYPE_QUERY, endpoint1));
    assertEquals(Set.of(parameter1), getSensitiveParameters(ParamType.PARAM_TYPE_BODY, endpoint2));
    assertEquals(Set.of(parameter4), getSensitiveParameters(ParamType.PARAM_TYPE_HEADER, ""));

    // this should not mark parameter2 as insensitive as it has already been marked sensitive and
    // onlyIfUnset is true
    markInsensitive(List.of(parameter2), endpoint1, true);
    assertEquals(
        Set.of(parameter1, parameter2),
        getSensitiveParameters(ParamType.PARAM_TYPE_BODY, endpoint1));
    assertEquals(Set.of(), getInsensitiveParameters(ParamType.PARAM_TYPE_BODY, endpoint1));

    // this should mark parameter2 as insensitive as onlyIfUnset is false
    markInsensitive(List.of(parameter2), endpoint1, false);
    assertEquals(Set.of(parameter1), getSensitiveParameters(ParamType.PARAM_TYPE_BODY, endpoint1));
    assertEquals(
        Set.of(parameter2), getInsensitiveParameters(ParamType.PARAM_TYPE_BODY, endpoint1));

    // update redaction strategy of parameter1
    updateRedactionStrategy(parameter1, REDACTION_STRATEGY_OBFUSCATE);
    assertEquals(
        Set.of(
            ParameterWithRedactionStrategy.newBuilder()
                .setParameter(parameter1)
                .setRedactionStrategy(REDACTION_STRATEGY_OBFUSCATE)
                .build()),
        getRedactionStrategy(ParamType.PARAM_TYPE_BODY, endpoint1));

    // parameter3 and parameter 4 have been marked as sensitive and their redaction strategy comes
    // from default pii filter config
    assertEquals(
        Set.of(
            ParameterWithRedactionStrategy.newBuilder()
                .setParameter(parameter3)
                .setRedactionStrategy(REDACTION_STRATEGY_HASH)
                .build()),
        getRedactionStrategy(ParamType.PARAM_TYPE_QUERY, endpoint1));
    assertEquals(
        Set.of(
            ParameterWithRedactionStrategy.newBuilder()
                .setParameter(parameter4)
                .setRedactionStrategy(REDACTION_STRATEGY_OBFUSCATE)
                .build()),
        getRedactionStrategy(ParamType.PARAM_TYPE_HEADER, ""));
  }

  @Test
  void testPiiFilterConfig() throws InvalidProtocolBufferException {
    Parameter parameter1 = getParameter(ParamType.PARAM_TYPE_BODY, "p1");
    Parameter parameter2 = getParameter(ParamType.PARAM_TYPE_HEADER, "p2");

    updateRedactionStrategy(parameter1, REDACTION_STRATEGY_OBFUSCATE);
    updateRedactionStrategy(parameter2, REDACTION_STRATEGY_HASH);
    PiiElement piiElement1 = getPiiElement(parameter1.getName(), REDACTION_STRATEGY_OBFUSCATE);
    PiiElement piiElement2 = getPiiElement(parameter2.getName(), REDACTION_STRATEGY_HASH);
    PiiFilterConfig expected = getExpectedPiiFilterConfig(List.of(piiElement1, piiElement2));
    PiiFilterConfig actual = getPiiFilterConfig();
    assertEquals(
        new HashSet<>(expected.getKeyRegexsList()), new HashSet<>(actual.getKeyRegexsList()));

    updateRedactionStrategy(parameter2, REDACTION_STRATEGY_OBFUSCATE);
    piiElement2 = getPiiElement(parameter2.getName(), REDACTION_STRATEGY_OBFUSCATE);
    expected = getExpectedPiiFilterConfig(List.of(piiElement1, piiElement2));
    actual = getPiiFilterConfig();
    assertEquals(
        new HashSet<>(expected.getKeyRegexsList()), new HashSet<>(actual.getKeyRegexsList()));
  }

  private Parameter getParameter(ParamType paramType, String paramName) {
    return Parameter.newBuilder().setParamType(paramType).setName(paramName).build();
  }

  private void markSensitive(List<Parameter> parameters, String endpoint, boolean onlyIfUnset) {
      markParameters(parameters, endpoint, true, onlyIfUnset);
  }

  private void markInsensitive(
      List<Parameter> parameters, String endpoint, boolean onlyIfUnset) {
    markParameters(parameters, endpoint, false, onlyIfUnset);
  }

  private void markParameters(
      List<Parameter> parameters, String endpoint, boolean sensitive, boolean onlyIfUnset) {
    MarkParametersRequest request =
        MarkParametersRequest.newBuilder()
            .addAllParameters(parameters)
            .setEndpoint(endpoint)
            .setSensitive(sensitive)
            .setOnlyIfUnset(onlyIfUnset)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1", () -> sensitiveDataConfigServiceStub.markParameters(request));
  }

  private Set<Parameter> getSensitiveParameters(ParamType paramType, String endpoint) {
    return getParameters(paramType, endpoint, true);
  }

  private Set<Parameter> getInsensitiveParameters(ParamType paramType, String endpoint) {
    return getParameters(paramType, endpoint, false);
  }

  private Set<Parameter> getParameters(ParamType paramType, String endpoint, boolean sensitive) {
    GetParametersRequest request =
        GetParametersRequest.newBuilder()
            .setFilter(
                Filter.newBuilder()
                    .setParamType(paramType)
                    .setEndpoint(endpoint)
                    .setSensitive(sensitive)
                    .build())
            .build();
    GetParametersResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> sensitiveDataConfigServiceStub.getParameters(request));
    return response.getParametersWithSensitivityList().stream()
        .map(ParameterWithSensitivity::getParameter)
        .collect(Collectors.toSet());
  }

  private void updateRedactionStrategy(Parameter parameter, RedactionStrategy redactionStrategy) {
    UpdateRedactionStrategyRequest request =
        UpdateRedactionStrategyRequest.newBuilder()
            .setParameter(parameter)
            .setRedactionStrategy(redactionStrategy)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        "tenant1", () -> sensitiveDataConfigServiceStub.updateRedactionStrategy(request));
  }

  private Set<ParameterWithRedactionStrategy> getRedactionStrategy(
      ParamType paramType, String endpoint) {
    GetRedactionStrategyRequest request =
        GetRedactionStrategyRequest.newBuilder()
            .setFilter(Filter.newBuilder().setParamType(paramType).setEndpoint(endpoint).build())
            .build();
    GetRedactionStrategyResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> sensitiveDataConfigServiceStub.getRedactionStrategy(request));
    return new HashSet<>(response.getParametersWithRedactionStrategyList());
  }

  private PiiElement getPiiElement(String paramName, RedactionStrategy redactionStrategy) {
    return PiiElement.newBuilder()
        .setRegex(paramName)
        .setRedactionStrategy(redactionStrategy)
        .build();
  }

  private PiiFilterConfig getPiiFilterConfig() {
    GetPiiFilterConfigRequest request = GetPiiFilterConfigRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            "tenant1", () -> piiFilterConfigServiceStub.getPiiFilterConfig(request))
        .getPiiFilterConfig();
  }

  private PiiFilterConfig getExpectedPiiFilterConfig(List<PiiElement> piiElementsAddedByUser)
      throws InvalidProtocolBufferException {
    ConfigClient configClient =
        IntegrationTestConfigClientFactory.getConfigClientForService(SERVICE_NAME);
    Config piiFilterConfig = configClient.getConfig().getConfig(DEFAULT_PII_FILTER_CONFIG);
    String jsonString = piiFilterConfig.root().render(ConfigRenderOptions.concise());
    PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    PiiFilterConfig defaultPiiFilterConfig = builder.build();
    PiiFilterConfig expectedPiiFilterConfig =
        PiiFilterConfig.newBuilder(defaultPiiFilterConfig)
            .addAllKeyRegexs(piiElementsAddedByUser)
            .build();
    return expectedPiiFilterConfig;
  }
}
