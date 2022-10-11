package ai.traceable.localprocessing.config.service.apinaming.http;

import static ai.traceable.span.processing.config.service.v1.Field.FIELD_ENVIRONMENT_NAME;
import static ai.traceable.span.processing.config.service.v1.RelationalOperator.RELATIONAL_OPERATOR_IN;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ApiNamingTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.LocalTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdRegexConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrieModelTrainingConfig;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.ApiNamingPattern;
import ai.traceable.localprocessing.config.service.v1.DiffLog;
import ai.traceable.localprocessing.config.service.v1.DiffPattern;
import ai.traceable.localprocessing.config.service.v1.FullPattern;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingConfig;
import ai.traceable.localprocessing.config.service.v1.HttpApiNamingCustomRule;
import ai.traceable.localprocessing.config.service.v1.Wildcard;
import ai.traceable.platform.apientity.Addition;
import ai.traceable.platform.apientity.Deletion;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieDiffLog;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.http.model.TrieNodeConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRule;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleConfig;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import ai.traceable.span.processing.config.service.v1.ApiNamingRuleInfo;
import ai.traceable.span.processing.config.service.v1.ApiSpecBasedConfig;
import ai.traceable.span.processing.config.service.v1.GetAllApiNamingRulesResponse;
import ai.traceable.span.processing.config.service.v1.ListValue;
import ai.traceable.span.processing.config.service.v1.RelationalSpanFilterExpression;
import ai.traceable.span.processing.config.service.v1.SegmentMatchingBasedConfig;
import ai.traceable.span.processing.config.service.v1.SpanFilter;
import ai.traceable.span.processing.config.service.v1.SpanFilterValue;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class ApiNamingManagerTestUtils {
  private static final String SERVICE_ID1 = "serviceId1";
  private static final String SERVICE_ID2 = "serviceId2";

  public static HttpApiNamingConfig buildApiNamingConfig() {
    UuidGenerator uuidGenerator = new UuidGenerator();
    HttpApiNamingConfig.Builder httpApiNamingConfigBuilder = HttpApiNamingConfig.newBuilder();
    httpApiNamingConfigBuilder
        .addSegmentWhitelistRegexes("allowRegex")
        .addAllApiNamingCustomRules(
            List.of(
                buildTestApiNamingRule("id-regex-1", "replacement-value-1"),
                buildTestApiNamingRule("id-regex-2", "replacement-value-2")))
        .addAllFallbackWildcardRegexes(
            List.of(
                "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
                "\\d+"));
    String hash = uuidGenerator.generateId(httpApiNamingConfigBuilder.build());
    return httpApiNamingConfigBuilder.setHash(hash).build();
  }

  public static GetAllScopedTrainingConfigsResponse buildGetAllScopedTrainingConfigsResponse() {
    return GetAllScopedTrainingConfigsResponse.newBuilder()
        .addAllScopedTrainingConfigs(
            List.of(
                buildScopedTrainingConfig(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(
                            AnomalyServiceScope.newBuilder().setId(SERVICE_ID1).build())
                        .build()),
                buildScopedTrainingConfig(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(
                            AnomalyServiceScope.newBuilder().setId(SERVICE_ID2).build())
                        .build()),
                buildScopedTrainingConfig(
                    AnomalyConfigScope.newBuilder()
                        .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                        .build())))
        .build();
  }

  private static ScopedTrainingConfig buildScopedTrainingConfig(
      AnomalyConfigScope anomalyConfigScope) {
    return ScopedTrainingConfig.newBuilder()
        .setConfigScope(anomalyConfigScope)
        .addTrainingConfigs(
            TrainingConfig.newBuilder()
                .setDisabled(false)
                .setLocalTrainingConfig(
                    LocalTrainingConfig.newBuilder()
                        .setApiNamingConfig(ApiNamingConfig.getDefaultInstance())
                        .build())
                .build())
        .addTrainingConfigs(
            TrainingConfig.newBuilder()
                .setApiNamingTrainingConfig(ApiNamingTrainingConfig.newBuilder().build()))
        .addTrainingConfigs(
            TrainingConfig.newBuilder()
                .setApiNamingTrainingConfig(
                    ApiNamingTrainingConfig.newBuilder()
                        .setTrieModelTrainingConfig(
                            TrieModelTrainingConfig.newBuilder()
                                .setEmbryonicThreshold(123)
                                .setMaxNumberOfTriePaths(1234)
                                .setAllowRegexList(
                                    StringList.newBuilder().addValues("allowRegex").build())
                                .setExtensions(
                                    StringList.newBuilder().addValues("extension").build())
                                .setIds(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder().addValues("regexId").build())
                                        .setThreshold(1)
                                        .build())
                                .setLowCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder().addValues("regexLow").build())
                                        .setThreshold(1)
                                        .build())
                                .setMediumCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder()
                                                .addValues("regexMedium")
                                                .build())
                                        .setThreshold(1)
                                        .build())
                                .setHighCardinality(
                                    ThresholdRegexConfig.newBuilder()
                                        .setRegexList(
                                            StringList.newBuilder().addValues("regexHigh").build())
                                        .setThreshold(1)
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  public static GetAllApiNamingRulesResponse buildGetAllApiNamingRuleResponse() {
    return GetAllApiNamingRulesResponse.newBuilder()
        .addRuleDetails(
            ApiNamingRuleDetails.newBuilder()
                .setRule(
                    ApiNamingRule.newBuilder()
                        .setRuleInfo(
                            ApiNamingRuleInfo.newBuilder()
                                .setDisabled(false)
                                .setFilter(buildTestFilter())
                                .setRuleConfig(
                                    ApiNamingRuleConfig.newBuilder()
                                        .setSegmentMatchingBasedConfig(
                                            SegmentMatchingBasedConfig.newBuilder()
                                                .addRegexes("id-regex-1")
                                                .addValues("replacement-value-1")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .addRuleDetails(
            ApiNamingRuleDetails.newBuilder()
                .setRule(
                    ApiNamingRule.newBuilder()
                        .setRuleInfo(
                            ApiNamingRuleInfo.newBuilder()
                                .setDisabled(false)
                                .setFilter(buildTestFilter())
                                .setRuleConfig(
                                    ApiNamingRuleConfig.newBuilder()
                                        .setApiSpecBasedConfig(
                                            ApiSpecBasedConfig.newBuilder()
                                                .addRegexes("id-regex-2")
                                                .addValues("replacement-value-2")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  public static List<List<Segment>> builtNonEmbryonicPaths =
      List.of(
          List.of(
              Segment.newBuilder().setName("GET").build(),
              Segment.newBuilder().setName("a").build(),
              Segment.newBuilder().setName("b").build()),
          List.of(
              Segment.newBuilder().setName("POST").build(),
              Segment.newBuilder().setName("a").build(),
              Segment.newBuilder()
                  .setName(
                      ai.traceable.platform.apientity.Wildcard.newBuilder()
                          .setWildcardType(TrieNodeType.ID)
                          .setExtension("e")
                          .build())
                  .build()));

  public static TrieNodeConfig builtTrieNodeConfig =
      new TrieNodeConfig(
          List.of("allowRegex"),
          List.of("regexId"),
          List.of("regexLow"),
          List.of("regexHigh"),
          new HashSet<>(List.of("extension")),
          123);

  public static FullPattern builtExpectedFullPattern =
      FullPattern.newBuilder()
          .addAllApiNamingPatterns(
              List.of(
                  ApiNamingPattern.newBuilder()
                      .addAllSegments(
                          List.of(
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setName("GET")
                                  .build(),
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setName("a")
                                  .build(),
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setName("b")
                                  .build()))
                      .build(),
                  ApiNamingPattern.newBuilder()
                      .addAllSegments(
                          List.of(
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setName("POST")
                                  .build(),
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setName("a")
                                  .build(),
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setWildcard(
                                      Wildcard.newBuilder()
                                          .setIdentificationRegex("regexId.e")
                                          .setReplacementPattern("*.e")
                                          .build())
                                  .build()))
                      .build()))
          .build();

  public static TrieDiffLog builtTrieDiffLog =
      TrieDiffLog.newBuilder()
          .setPathAdditions(
              List.of(
                  Addition.newBuilder()
                      .setSegments(
                          List.of(
                              Segment.newBuilder().setName("GET").build(),
                              Segment.newBuilder()
                                  .setName(
                                      ai.traceable.platform.apientity.Wildcard.newBuilder()
                                          .setWildcardType(TrieNodeType.ID)
                                          .setExtension("e")
                                          .build())
                                  .build()))
                      .build()))
          .setNodeDeletions(
              List.of(
                  Deletion.newBuilder()
                      .setSegments(List.of(Segment.newBuilder().setName("GET").build()))
                      .build(),
                  Deletion.newBuilder()
                      .setSegments(
                          List.of(
                              Segment.newBuilder()
                                  .setName(
                                      ai.traceable.platform.apientity.Wildcard.newBuilder()
                                          .setWildcardType(TrieNodeType.ID)
                                          .setExtension("ex")
                                          .build())
                                  .build()))
                      .build()))
          .build();

  public static DiffPattern builtExpectedDiffTrie =
      DiffPattern.newBuilder()
          .addDiffLogs(
              DiffLog.newBuilder()
                  .setApiNamingPatternAddition(
                      ApiNamingPattern.newBuilder()
                          .addSegments(
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setName("GET")
                                  .build())
                          .addSegments(
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setWildcard(
                                      Wildcard.newBuilder()
                                          .setIdentificationRegex("regexId.e")
                                          .setReplacementPattern("*.e")
                                          .build())
                                  .build())
                          .build())
                  .build())
          .addDiffLogs(
              DiffLog.newBuilder()
                  .setApiNamingPatternDeletion(
                      ApiNamingPattern.newBuilder()
                          .addSegments(
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setName("GET")
                                  .build())
                          .build())
                  .build())
          .addDiffLogs(
              DiffLog.newBuilder()
                  .setApiNamingPatternDeletion(
                      ApiNamingPattern.newBuilder()
                          .addSegments(
                              ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                                  .setWildcard(
                                      Wildcard.newBuilder()
                                          .setIdentificationRegex("regexId.ex")
                                          .setReplacementPattern("*.ex")
                                          .build())
                                  .build())
                          .build())
                  .build())
          .build();

  public static Config buildFullTrieReloadConfig(String tenantScopedVersion) {
    return ConfigFactory.parseMap(
        Map.of(
            "default",
            Map.of("version", "0.0.0"),
            "tenantId",
            Map.of("default", Map.of("version", tenantScopedVersion))));
  }

  private static HttpApiNamingCustomRule buildTestApiNamingRule(
      String identificationRegex, String replacementValue) {
    return HttpApiNamingCustomRule.newBuilder()
        .setApiNamingPattern(
            ApiNamingPattern.newBuilder()
                .addAllSegments(
                    List.of(
                        ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
                            .setWildcard(
                                Wildcard.newBuilder()
                                    .setIdentificationRegex(identificationRegex)
                                    .setReplacementPattern(replacementValue)
                                    .build())
                            .build()))
                .build())
        .build();
  }

  private static SpanFilter buildTestFilter() {
    return SpanFilter.newBuilder()
        .setRelationalSpanFilter(
            RelationalSpanFilterExpression.newBuilder()
                .setField(FIELD_ENVIRONMENT_NAME)
                .setOperator(RELATIONAL_OPERATOR_IN)
                .setRightOperand(
                    SpanFilterValue.newBuilder()
                        .setListValue(
                            ListValue.newBuilder()
                                .addValues(
                                    SpanFilterValue.newBuilder()
                                        .setStringValue("environment")
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }
}
