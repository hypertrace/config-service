package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.EMAIL_DOMAIN_CONDITION;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.USER_ID_VALUE_LHS;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildContainsOperatorMatchCondition;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.ratelimiting.service.v2.rules.converter.condition.ConverterUtils.joinChildConditions;

import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.EmailDomainCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingEmailDomainConditionConverter implements RateLimitingConditionConverter {

  @Override
  public MatchConditionDetails buildMatchCondition(
      final RequestContext requestContext, final LeafCondition leafCondition) {
    final EmailDomainCondition emailDomainCondition = leafCondition.getEmailDomainCondition();
    List<MatchCondition.Builder> childMatchConditions = new ArrayList<>();
    if (!emailDomainCondition.getEmailDomainsList().isEmpty()) {
      childMatchConditions.addAll(
          buildContainsOperatorMatchCondition(
              USER_ID_VALUE_LHS, emailDomainCondition.getEmailDomainsList()));
    }
    if (!emailDomainCondition.getEmailRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_ID_VALUE_LHS, emailDomainCondition.getEmailRegexesList()));
    }
    return new MatchConditionDetails(
        joinChildConditions(childMatchConditions, emailDomainCondition.getExclude()),
        Collections.emptyList(),
        Collections.emptyList());
  }

  @Override
  public ConditionCase getConditionCase() {
    return EMAIL_DOMAIN_CONDITION;
  }
}
