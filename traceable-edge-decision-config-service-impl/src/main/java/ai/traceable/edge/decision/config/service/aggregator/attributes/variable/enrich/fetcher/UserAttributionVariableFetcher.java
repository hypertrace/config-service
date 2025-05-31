package ai.traceable.edge.decision.config.service.aggregator.attributes.variable.enrich.fetcher;

import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.UserAttributionRuleFetcher;
import com.google.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class UserAttributionVariableFetcher implements VariableFetcher {
  private static final DerivationRule IP_ADDRESS_DERIVATION_RULE =
      DerivationRule.newBuilder()
          .setTransformationConfig(
              DataTransformationConfig.newBuilder()
                  .setJexlExpression(
                      JexlExpressionConfig.newBuilder().setJexlExpression("$s.getIpAddress()"))
                  .setOutputType(FieldType.FIELD_TYPE_STR))
          .build();
  private final UserAttributionRuleFetcher userAttributionRuleFetcher;

  @Inject
  public UserAttributionVariableFetcher(UserAttributionRuleFetcher userAttributionRuleFetcher) {
    this.userAttributionRuleFetcher = userAttributionRuleFetcher;
  }

  public VariableDerivationMapping getVariable(RequestContext requestContext) {
    return VariableDerivationMapping.newBuilder()
        .setName(USER_ATTRIBUTION_VARIABLE_NAME.getValue())
        .addAllRules(userAttributionRuleFetcher.getUserAttributionRules(requestContext))
        // fallbacks to IP address if user attribution is not available
        .addRules(IP_ADDRESS_DERIVATION_RULE)
        .build();
  }
}
