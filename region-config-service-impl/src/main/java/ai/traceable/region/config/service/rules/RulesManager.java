package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  List<RegionRule> getRegionRules(RequestContext requestContext);

  RegionRule createRegionRule(
      RequestContext requestContext, CreateRegionRuleRequest createRuleRequest);

  RegionRule updateRegionRule(RequestContext requestContext, UpdateRegionRuleRequest request);

  RegionRule deleteRegionRule(RequestContext requestContext, String id)
      throws InvalidProtocolBufferException;
}
