package ai.traceable.jwt.extraction.config.service.converter;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class JwtExtractionEdgeDecisionConverter {

  public EdgeDecisionEngineConfig convert(
      final RequestContext requestContext, final List<JwtExtractionRule> jwtExtractionRules) {
    final EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    jwtExtractionRules.stream()
        .filter(jwtExtractionRule -> !jwtExtractionRule.getDisabled())
        .map(rule -> this.convertJwtExtractionRule(requestContext, rule))
        .forEach(builder::addAllDecisionRules);
    return builder.build();
  }

  private List<EdgeDecisionRule> convertJwtExtractionRule(
      final RequestContext requestContext, final JwtExtractionRule jwtExtractionRule) {
    return Collections.emptyList();
  }
}
