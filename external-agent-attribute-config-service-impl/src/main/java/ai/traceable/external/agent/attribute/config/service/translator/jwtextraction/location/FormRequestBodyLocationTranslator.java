package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_BODY_KEY_HTTP;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;

public class FormRequestBodyLocationTranslator extends AbstractLocationTranslator {

  @Override
  public List<LocationTranslationState> translateCase(JwtLocation location)
      throws JwtTranslationException {
    StringPredicate formPredicate = location.getFormRequestBody();
    if (formPredicate.getOperator()
        != StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS) {
      throw new JwtTranslationException(
          "Only EQUALS predicate operator supported for form body location");
    }
    return List.of(
        new LocationTranslationState(
            REQUEST_BODY_KEY_HTTP,
            Optional.of(formPredicate.getValue()),
            getRegexCaptureGroup(location),
            Optional.of(
                (attributeRule -> extractFromForm(formPredicate.getValue(), attributeRule)))));
  }

  private AttributeRule extractFromForm(String formKey, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder()
                .setUrlEncodedProjector(
                    AttributeRule.Projector.UrlEncodedProjector.newBuilder()
                        .setUrlParamRule(
                            AttributeRule.Projector.ParsedObjectKeyRule.newBuilder()
                                .setKey(formKey)
                                .setAttributeRule(childRule))))
        .build();
  }
}
