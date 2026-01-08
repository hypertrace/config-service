package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleRecord;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {

  List<IpRangeRule> getIpRangeRules(RequestContext requestContext, GetRulesFilter filter);

  List<IpRangeRuleRecord> getIpRangeRuleRecords(
      RequestContext requestContext, GetRulesFilter filter);

  IpRangeRule createIpRangeRule(
      RequestContext requestContext, CreateIpRangeRuleRequest createRuleRequest);

  IpRangeRule updateIpRangeRule(
      RequestContext requestContext, UpdateIpRangeRuleRequest updateIpRangeRuleRequest);

  Optional<IpRangeRule> deleteIpRangeRule(RequestContext requestContext, String id)
      throws InvalidProtocolBufferException;
}
