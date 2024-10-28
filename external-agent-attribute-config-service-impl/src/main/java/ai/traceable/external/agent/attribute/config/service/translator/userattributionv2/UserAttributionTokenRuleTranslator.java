package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.CustomProjection;
import ai.traceable.userattribution.config.service.v2.LiteralValueProjection;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
class UserAttributionTokenRuleTranslator {
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();

  private final AttributeRuleBuilder attributeRuleBuilder;
  private final AttributeTranslator attributeTranslator;
  private final PredicateTranslator predicateTranslator;
  private final ValueProjectionsTranslator valueProjectionsTranslator;

  Stream<AttributeRule> translateTokenRule(
      Optional<Predicate> predicateOptional,
      UserAttributionTokenRule tokenRule,
      UserAttributionRootTokenRule rootTokenRule,
      AttributeRule childAttributeRule) {
    return translateTokenRule(tokenRule, rootTokenRule, childAttributeRule)
        .map(
            attributeRule ->
                predicateOptional
                    .map(
                        predicate ->
                            AttributeRule.newBuilder()
                                .setProjector(
                                    AttributeRule.Projector.newBuilder()
                                        .setConditionalProjector(
                                            AttributeRule.Projector.ConditionalProjector
                                                .newBuilder()
                                                .setPredicate(predicate)
                                                .setAttributeRule(attributeRule)))
                                .build())
                    .orElse(attributeRule));
  }

  Stream<AttributeRule> translateTokenRule(
      UserAttributionTokenRule tokenRule,
      UserAttributionRootTokenRule rootTokenRule,
      AttributeRule childAttributeRule) {
    switch (tokenRule.getProjectionCase()) {
      case LITERAL_VALUE_PROJECTION:
        return Stream.of(
            predicateTranslator.addConditionalPredicateIfPresent(
                tokenRule,
                translateLiteralValueProjection(
                    tokenRule.getLiteralValueProjection(), childAttributeRule)));
      case CUSTOM_PROJECTION:
        return Stream.of(
            predicateTranslator.addConditionalPredicateIfPresent(
                tokenRule, translateCustomProjection(tokenRule.getCustomProjection())));
      case ATTRIBUTE_PROJECTION:
        AttributeProjection attributeProjection = tokenRule.getAttributeProjection();
        return translateAttributeProjection(
                attributeProjection.getAttribute(),
                attributeProjection.getValueProjectionsList(),
                childAttributeRule)
            .map(
                attributeRule ->
                    predicateTranslator.addConditionalPredicateIfPresent(tokenRule, attributeRule));
      case ROOT_RELATIVE_PROJECTION:
        AttributeProjection rootAttributeProjection = rootTokenRule.getAttributeProjection();
        return translateAttributeProjection(
                rootAttributeProjection.getAttribute(),
                rootAttributeProjection.getValueProjectionsList(),
                tokenRule.getRootRelativeProjection().getValueProjectionsList(),
                childAttributeRule)
            .map(
                attributeRule ->
                    predicateTranslator.addConditionalPredicateIfPresent(
                        rootTokenRule, attributeRule));
      default:
        throw Status.INTERNAL
            .withDescription(
                String.format("Unknown projection case: %s", tokenRule.getProjectionCase()))
            .asRuntimeException();
    }
  }

  private AttributeRule translateLiteralValueProjection(
      LiteralValueProjection literalValueProjection, AttributeRule childAttributeRule) {
    return attributeRuleBuilder.buildStaticAttributeRule(
        literalValueProjection.getLiteralValue().getStringValue(), childAttributeRule);
  }

  @SneakyThrows
  private AttributeRule translateCustomProjection(CustomProjection customProjection) {
    AttributeRule.Builder builder = AttributeRule.newBuilder();
    PARSER.merge(customProjection.getCustomJson(), builder);
    return builder.build();
  }

  private Stream<AttributeRule> translateAttributeProjection(
      Attribute attribute,
      List<ValueProjection> valueProjections,
      AttributeRule childAttributeRule) {
    return translateAttributeProjection(
        attribute, valueProjections, Collections.emptyList(), childAttributeRule);
  }

  private Stream<AttributeRule> translateAttributeProjection(
      Attribute attribute,
      List<ValueProjection> valueProjections,
      List<ValueProjection> childValueProjections,
      AttributeRule childAttributeRule) {
    List<ValueProjection> allValueProjections = new ArrayList<>();
    allValueProjections.addAll(valueProjections);
    allValueProjections.addAll(childValueProjections);
    AttributeRule finalChildAttributeRule;
    if (allValueProjections.isEmpty()) {
      finalChildAttributeRule = childAttributeRule;
    } else {
      finalChildAttributeRule =
          valueProjectionsTranslator.translateValueProjections(
              allValueProjections, childAttributeRule);
    }
    return attributeTranslator.translateAttribute(attribute, finalChildAttributeRule);
  }
}
