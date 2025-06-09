package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition.DetectionExclusionRuleConditionConverter;
import ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition.DetectionExclusionRuleConditionModule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class DetectionExclusionRuleEdgeDecisionConverterTest {

  private static final String INPUT_DIR = "rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  static DetectionExclusionRuleEdgeDecisionConverter converter;

  @BeforeAll
  static void setup() {
    CachedServiceMappingProvider serviceEntityProvider = mock(CachedServiceMappingProvider.class);
    CachedServiceMappingProvider.ServiceIdentifierEntity service1 =
        new CachedServiceMappingProvider.ServiceIdentifierEntity(
            "serviceName1", Optional.of("environment1"));
    Map<String, Optional<CachedServiceMappingProvider.ServiceIdentifierEntity>>
        serviceIdentifierEntityMap = Map.of("serviceId1", Optional.of(service1));
    when(serviceEntityProvider.getServiceIdentifierEntities(REQUEST_CONTEXT, Set.of("serviceId1")))
        .thenReturn(serviceIdentifierEntityMap);
    CachedApiMappingProvider apiEntityProvider = mock(CachedApiMappingProvider.class);
    CachedApiMappingProvider.ApiIdentifierEntity api1 =
        new CachedApiMappingProvider.ApiIdentifierEntity(
            "apiId1", "apiName1", "/api1", List.of("/api1"), Collections.emptyList());
    Map<String, Optional<CachedApiMappingProvider.ApiIdentifierEntity>> apiIdentifierEntityMap =
        Map.of("apiId1", Optional.of(api1));
    Map<String, Set<CachedApiMappingProvider.ApiIdentifierEntity>> apiIdentifierEntityLabelMap =
        Map.of("labelId1", Set.of(api1));
    when(apiEntityProvider.getApiIdentifierEntities(REQUEST_CONTEXT, Set.of("apiId1")))
        .thenReturn(apiIdentifierEntityMap);
    when(apiEntityProvider.getApiIdentifierEntitiesHavingLabels(
            REQUEST_CONTEXT, Set.of("labelId1")))
        .thenReturn(apiIdentifierEntityLabelMap);

    Injector injector =
        Guice.createInjector(
            new DetectionExclusionRuleEdgeDecisionConverterTestModule(
                serviceEntityProvider, apiEntityProvider));
    Set<DetectionExclusionRuleConditionConverter> conditionConverters =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    converter = new DetectionExclusionRuleEdgeDecisionConverter(conditionConverters);
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testEvaluateRule(String fileName) throws InvalidProtocolBufferException {
    String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
    String expectedOutputFileStr = readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
    DetectionExclusionRule.Builder builder = DetectionExclusionRule.newBuilder();
    parser.merge(inputFileStr, builder);
    EdgeDecisionEngineConfig output = converter.convert(REQUEST_CONTEXT, List.of(builder.build()));
    EdgeDecisionEngineConfig.Builder expectedOutputBuilder = EdgeDecisionEngineConfig.newBuilder();
    parser.merge(expectedOutputFileStr, expectedOutputBuilder);

    assertEquals(expectedOutputBuilder.build(), output);
  }

  static List<String> getInputFileNames() {
    String folderName =
        Objects.requireNonNull(
                DetectionExclusionRuleEdgeDecisionConverterTest.class
                    .getClassLoader()
                    .getResource(INPUT_DIR))
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(Objects.requireNonNull(queriesFolder.listFiles()))
        .map(File::getName)
        .collect(Collectors.toUnmodifiableList());
  }

  private String readResourceFileAsString(String dirName, String fileName) {
    try {
      File file =
          new File(
              Objects.requireNonNull(
                      this.getClass()
                          .getClassLoader()
                          .getResource(dirName + File.separator + fileName))
                  .toURI());
      return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static class DetectionExclusionRuleEdgeDecisionConverterTestModule extends AbstractModule {

    CachedServiceMappingProvider serviceEntityProvider;
    CachedApiMappingProvider apiEntityProvider;

    public DetectionExclusionRuleEdgeDecisionConverterTestModule(
        CachedServiceMappingProvider serviceEntityProvider,
        CachedApiMappingProvider apiEntityProvider) {
      this.serviceEntityProvider = serviceEntityProvider;
      this.apiEntityProvider = apiEntityProvider;
    }

    @Override
    protected void configure() {
      bind(CachedServiceMappingProvider.class).toInstance(serviceEntityProvider);
      bind(CachedApiMappingProvider.class).toInstance(apiEntityProvider);
      install(new DetectionExclusionRuleConditionModule());
    }
  }
}
