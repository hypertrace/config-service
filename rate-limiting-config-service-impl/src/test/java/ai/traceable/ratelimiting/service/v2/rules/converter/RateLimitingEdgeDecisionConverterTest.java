package ai.traceable.ratelimiting.service.v2.rules.converter;

import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;
import static ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder.getEncodedRateLimitViolationInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.edge.decision.config.service.SpanAttributeHandler;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionType;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.platform.actor.v1.RateLimitCategory;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionConverter;
import ai.traceable.ratelimiting.service.v2.rules.converter.condition.RateLimitingConditionModule;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RateLimitingEdgeDecisionConverterTest {
  private static final String INPUT_DIR = "rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  static RateLimitingEdgeDecisionConverter converter;

  @BeforeAll
  static void setup() {
    CachedApiMappingProvider provider = mock(CachedApiMappingProvider.class);
    ApiIdentifierEntity api1 =
        new ApiIdentifierEntity(
            "apiId1", "apiName1", "/api1", List.of("/api1"), Collections.emptyList());
    Map<String, Optional<ApiIdentifierEntity>> apiIdentifierEntityMap =
        Map.of("apiId1", Optional.of(api1));
    Map<String, Set<ApiIdentifierEntity>> apiIdentifierEntityLabelMap =
        Map.of("labelId1", Set.of(api1));
    when(provider.getApiIdentifierEntities(REQUEST_CONTEXT, Set.of("apiId1")))
        .thenReturn(apiIdentifierEntityMap);
    when(provider.getApiIdentifierEntitiesHavingLabels(REQUEST_CONTEXT, Set.of("labelId1")))
        .thenReturn(apiIdentifierEntityLabelMap);
    Injector injector =
        Guice.createInjector(new RateLimitingEdgeDecisionConverterTestModule(provider));
    Set<RateLimitingConditionConverter> conditionConverters =
        injector.getInstance(Key.get(new TypeLiteral<Set<RateLimitingConditionConverter>>() {}));
    converter = new RateLimitingEdgeDecisionConverter(conditionConverters);
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testEvaluateRule(String fileName) throws InvalidProtocolBufferException {
    String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
    String expectedOutputFileStr = readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
    RateLimitingRule.Builder rateLimitingRuleBuilder = RateLimitingRule.newBuilder();
    parser.merge(inputFileStr, rateLimitingRuleBuilder);
    RateLimitingRule rateLimitingRule = rateLimitingRuleBuilder.build();
    EdgeDecisionEngineConfig output = converter.convert(REQUEST_CONTEXT, List.of(rateLimitingRule));
    EdgeDecisionEngineConfig.Builder expectedOutputBuilder = EdgeDecisionEngineConfig.newBuilder();
    parser.merge(expectedOutputFileStr, expectedOutputBuilder);
    // Add span attributes to expected rules
    List<EdgeDecisionRule> updatedRules =
        expectedOutputBuilder.getDecisionRulesList().stream()
            .map(
                rule -> {
                  if (rule.getRuleDecision()
                      .getEdgeDecisionType()
                      .equals(EdgeDecisionType.EDGE_DECISION_TYPE_ALLOW)) {
                    return rule;
                  }
                  EdgeDecision updatedEdgeDecision =
                      rule.getRuleDecision().toBuilder()
                          .addAllSpanAttributes(
                              SpanAttributeHandler.getSpanAttributeDecorations(
                                  rule.getId(),
                                  false,
                                  EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT,
                                  getEncodedRateLimitViolationInfo(
                                      "",
                                      rule.getId(),
                                      rule.getName(),
                                      getRateLimitCategory(
                                          rateLimitingRule.getData().getCategory()),
                                      Map.of())))
                          .build();
                  return rule.toBuilder().setRuleDecision(updatedEdgeDecision).build();
                })
            .collect(Collectors.toList());

    assertEquals(
        expectedOutputBuilder.clearDecisionRules().addAllDecisionRules(updatedRules).build(),
        output);
  }

  private RateLimitCategory getRateLimitCategory(Category category) {
    switch (category) {
      case CATEGORY_RATE_LIMITING:
        return RateLimitCategory.RATE_LIMIT_CATEGORY_RATE_LIMITING;
      case CATEGORY_ENUMERATION:
        return RateLimitCategory.RATE_LIMIT_CATEGORY_ENUMERATION;
      case CATEGORY_DATA_EXFILTRATION:
        return RateLimitCategory.RATE_LIMIT_CATEGORY_DATA_EXFILTRATION;
      default:
        throw new IllegalArgumentException("Unknown category: " + category);
    }
  }

  static List<String> getInputFileNames() {
    String folderName =
        RateLimitingEdgeDecisionConverterTest.class
            .getClassLoader()
            .getResource(INPUT_DIR)
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(queriesFolder.listFiles())
        .map(file -> file.getName())
        .collect(Collectors.toUnmodifiableList());
  }

  private String readResourceFileAsString(String dirName, String fileName) {
    try {
      File file =
          new File(
              this.getClass()
                  .getClassLoader()
                  .getResource(dirName + File.separator + fileName)
                  .toURI());
      return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static class RateLimitingEdgeDecisionConverterTestModule extends AbstractModule {

    CachedApiMappingProvider provider;

    public RateLimitingEdgeDecisionConverterTestModule(CachedApiMappingProvider provider) {
      this.provider = provider;
    }

    @Override
    protected void configure() {
      bind(CachedApiMappingProvider.class).toInstance(provider);
      install(new RateLimitingConditionModule());
    }
  }
}
