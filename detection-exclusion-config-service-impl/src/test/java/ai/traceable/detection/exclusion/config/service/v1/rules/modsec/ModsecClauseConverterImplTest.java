package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EntityType;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.UrlScope;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ModsecClauseResult;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ServiceDetail;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ModsecClauseConverterImplTest {
  private static final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @Mock CachedServiceMappingProvider cachedServiceMappingProvider;

  private ModsecClauseConverterImpl modsecClauseConverter;

  @BeforeEach
  void setup() {
    // Initialize mocks
    MockitoAnnotations.openMocks(this);

    modsecClauseConverter = new ModsecClauseConverterImpl(cachedServiceMappingProvider);
    when(cachedServiceMappingProvider.getServiceIdentifierEntity(requestContext, "service-id-1"))
        .thenReturn(Optional.of(new ServiceIdentifierEntity("service-name-1", Optional.empty())));

    when(cachedServiceMappingProvider.getServiceIdentifierEntity(requestContext, "service-id-2"))
        .thenReturn(Optional.of(new ServiceIdentifierEntity("service-name-2", Optional.empty())));
  }

  @Test
  void testConvert_UrlScopeCondition() {
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setUrlScope(
                                        UrlScope.newBuilder()
                                            .addUrlRegexes(".*apple.*")
                                            .addUrlRegexes(".*mango.*"))
                                    .setExclude(false)))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        EntityScope.newBuilder()
                                            .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                            .addEntityIds("service-id-1")
                                            .addEntityIds("service-id-2"))
                                    .setExclude(false))))
            .build();

    ModsecClauseResult result = modsecClauseConverter.convert(requestContext, rule);
    assertEquals(
        List.of(
            new ServiceDetail("service-name-1", false), new ServiceDetail("service-name-2", false)),
        result.getServiceDetails());
    List<Clause> clauses = result.getClauses();

    assertEquals(1, clauses.size());
    Clause clause = clauses.get(0);
    assertEquals(MatchKey.MATCH_KEY_URL, clause.getMatchExpression().getMatchKey());
    assertEquals(".*apple.*|.*mango.*", clause.getMatchExpression().getMatchValue());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_MATCHES_REGEX, clause.getMatchExpression().getMatchOperator());
  }

  @Test
  void testConvert_UrlScopeCondition_exclude() {
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setUrlScope(
                                        UrlScope.newBuilder()
                                            .addUrlRegexes(".*apple.*")
                                            .addUrlRegexes(".*mango.*"))
                                    .setExclude(true)))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        EntityScope.newBuilder()
                                            .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                            .addEntityIds("service-id-1")
                                            .addEntityIds("service-id-2"))
                                    .setExclude(true))))
            .build();

    ModsecClauseResult result = modsecClauseConverter.convert(requestContext, rule);
    assertEquals(
        List.of(
            new ServiceDetail("service-name-1", true), new ServiceDetail("service-name-2", true)),
        result.getServiceDetails());
    List<Clause> clauses = result.getClauses();

    assertEquals(2, clauses.size());
    Clause clause = clauses.get(0);
    assertEquals(MatchKey.MATCH_KEY_URL, clause.getMatchExpression().getMatchKey());
    assertEquals(".*mango.*", clause.getMatchExpression().getMatchValue());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
        clause.getMatchExpression().getMatchOperator());
    clause = clauses.get(1);
    assertEquals(MatchKey.MATCH_KEY_URL, clause.getMatchExpression().getMatchKey());
    assertEquals(".*apple.*", clause.getMatchExpression().getMatchValue());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
        clause.getMatchExpression().getMatchOperator());
  }

  @Test
  void testConvert_AttributeMatchCondition_KeyValue() {
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setAttributeMatchCondition(
                                SpanAttributeMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMetadataMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_REQUEST_HEADER)
                                            .setMatchCondition(
                                                MatchCondition.newBuilder()
                                                    .setOperator(
                                                        ai.traceable.detection.exclusion.config
                                                            .service.v1.MatchOperator
                                                            .MATCH_OPERATOR_EQUALS)
                                                    .setValue(
                                                        Value.newBuilder().setStringValue("key"))))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(
                                                ai.traceable.detection.exclusion.config.service.v1
                                                    .MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                Value.newBuilder().setStringValue("value"))))))
            .build();

    List<Clause> result = modsecClauseConverter.convert(requestContext, rule).getClauses();

    assertEquals(1, result.size());
    Clause clause = result.get(0);
    assertEquals(KeyValueTag.KEY_VALUE_TAG_HEADER, clause.getKeyValueExpression().getTag());
    assertEquals("key", clause.getKeyValueExpression().getMatchKey());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_EQUALS, clause.getKeyValueExpression().getKeyMatchOperator());
    assertEquals("value", clause.getKeyValueExpression().getMatchValue());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
        clause.getKeyValueExpression().getValueMatchOperator());
    assertEquals(
        MatchCategory.MATCH_CATEGORY_REQUEST, clause.getKeyValueExpression().getMatchCategory());
  }

  @Test
  void testConvert_AttributeMatchCondition_OnlyKey() {
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setAttributeMatchCondition(
                                SpanAttributeMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMetadataMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_HOST)
                                            .setMatchCondition(
                                                MatchCondition.newBuilder()
                                                    .setOperator(
                                                        ai.traceable.detection.exclusion.config
                                                            .service.v1.MatchOperator
                                                            .MATCH_OPERATOR_NOT_MATCH_REGEX)
                                                    .setValue(
                                                        Value.newBuilder()
                                                            .setStringValue("key")))))))
            .build();

    List<Clause> result = modsecClauseConverter.convert(requestContext, rule).getClauses();

    assertEquals(1, result.size());
    Clause clause = result.get(0);
    assertEquals(MatchKey.MATCH_KEY_HOST, clause.getMatchExpression().getMatchKey());
    assertEquals("key", clause.getMatchExpression().getMatchValue());
    assertEquals(1, result.size());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
        clause.getMatchExpression().getMatchOperator());
    assertEquals(
        MatchCategory.MATCH_CATEGORY_REQUEST, clause.getMatchExpression().getMatchCategory());
  }

  @Test
  void testConvert_SimpleUrlValueMatchCondition() {
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setAttributeMatchCondition(
                                SpanAttributeMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMetadataMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_URL))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(
                                                ai.traceable.detection.exclusion.config.service.v1
                                                    .MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                Value.newBuilder().setStringValue(".*sf.*"))))))
            .build();

    List<Clause> result = modsecClauseConverter.convert(requestContext, rule).getClauses();

    assertEquals(1, result.size());
    Clause clause = result.get(0);
    assertEquals(MatchKey.MATCH_KEY_URL, clause.getMatchExpression().getMatchKey());
    assertEquals(".*sf.*", clause.getMatchExpression().getMatchValue());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_MATCHES_REGEX, clause.getMatchExpression().getMatchOperator());
  }

  @Test
  void testConvert_SimpleUserAgentMatchCondition() {
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setAttributeMatchCondition(
                                SpanAttributeMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMetadataMatchCondition.newBuilder()
                                            .setMetadata(KeyMetadata.KEY_METADATA_USER_AGENT))
                                    .setValueMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(
                                                ai.traceable.detection.exclusion.config.service.v1
                                                    .MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setValue(
                                                Value.newBuilder()
                                                    .setStringValue("user-agent-val"))))))
            .build();

    List<Clause> modsecClauses =
        modsecClauseConverter.convert(requestContext, detectionExclusionRule).getClauses();

    assertEquals(1, modsecClauses.size());
    Clause clause = modsecClauses.get(0);
    assertEquals(MatchKey.MATCH_KEY_HEADER_NAME, clause.getMatchExpression().getMatchKey());
    assertEquals("user-agent-val", clause.getMatchExpression().getMatchValue());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_EQUALS, clause.getMatchExpression().getMatchOperator());
  }
}
