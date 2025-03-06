package ai.traceable.edge.decision.config.service.aggregator.attributes;

import static ai.traceable.edge.decision.config.service.VariableConstants.USER_ATTRIBUTION_VARIABLE_NAME;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.UserAttributionFetcher;
import com.google.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class UserAttributionVariableEnricher implements VariableEnricherBase {
  private static final DerivationRule IP_ADDRESS_DERIVATION_RULE =
      DerivationRule.newBuilder()
          .setTransformationConfig(
              DataTransformationConfig.newBuilder()
                  .setJexlExpression(
                      JexlExpressionConfig.newBuilder().setJexlExpression("$s.getIpAddress"))
                  .setOutputType(FieldType.FIELD_TYPE_STR))
          .build();
  private final UserAttributionFetcher userAttributionFetcher;

  @Inject
  public UserAttributionVariableEnricher(UserAttributionFetcher userAttributionFetcher) {
    this.userAttributionFetcher = userAttributionFetcher;
  }

  public VariableDerivationMapping getVariable(RequestContext requestContext) {
    return VariableDerivationMapping.newBuilder()
        .setName(USER_ATTRIBUTION_VARIABLE_NAME.getValue())
        .addAllRules(userAttributionFetcher.getUserAttributionRules(requestContext))
        // fallbacks to IP address if user attribution is not available
        .addRules(IP_ADDRESS_DERIVATION_RULE)
        .build();
  }
}
