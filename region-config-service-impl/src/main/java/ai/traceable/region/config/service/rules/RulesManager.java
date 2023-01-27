package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  List<RegionRule> getRegionRules(RequestContext requestContext, GetRegionRulesFilter filter);

  Optional<RegionRule> createRegionRule(
      RequestContext requestContext, CreateRegionRuleRequest createRuleRequest);

  Optional<RegionRule> updateRegionRule(
      RequestContext requestContext, UpdateRegionRuleRequest request);

  Optional<RegionRule> deleteRegionRule(RequestContext requestContext, String id)
      throws InvalidProtocolBufferException;
}
