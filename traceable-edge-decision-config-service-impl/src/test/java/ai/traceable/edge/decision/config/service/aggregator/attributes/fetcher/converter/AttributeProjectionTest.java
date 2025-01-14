package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.AttributeProjection;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value.ValueProjection;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value.ValueProjectionFactory;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.KeyMatchOperator;
import ai.traceable.userattribution.config.service.v2.PayloadMatch;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class AttributeProjectionTest {

  @Test
  void testApply_withRequestCookieSimple() {
    KeyMatch keyMatch =
        KeyMatch.newBuilder()
            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_STARTS_WITH)
            .setMatchKey("x-user-id")
            .build();
    Attribute attribute = Attribute.newBuilder().setRequestCookie(keyMatch).build();

    ai.traceable.userattribution.config.service.v2.AttributeProjection inputProjection =
        ai.traceable.userattribution.config.service.v2.AttributeProjection.newBuilder()
            .setAttribute(attribute)
            .build();

    AttributeProjection attributeProjection = new AttributeProjection(inputProjection);

    String inputJexlExpression = "$s";

    String expectedJexl =
        "map:matchingValue("
            + "$s.getRequestCookies(), "
            + "predicate:startsWith('x-user-id')"
            + ")";

    assertEquals(expectedJexl, attributeProjection.apply(inputJexlExpression));
  }

  @Test
  void testApply_withRequestHeader() {
    KeyMatch keyMatch =
        KeyMatch.newBuilder()
            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_MATCHES_REGEX)
            .setMatchKey("x-user-.*")
            .build();
    Attribute attribute = Attribute.newBuilder().setRequestHeader(keyMatch).build();
    ai.traceable.userattribution.config.service.v2.AttributeProjection inputProjection =
        ai.traceable.userattribution.config.service.v2.AttributeProjection.newBuilder()
            .setAttribute(attribute)
            .build();

    AttributeProjection attributeProjection = new AttributeProjection(inputProjection);

    String inputJexlExpression = "$s";
    String expectedJexl =
        "map:matchingValue("
            + "$s.getRequestHeaders(), "
            + "predicate:matchesRegex('x-user-.*')"
            + ")";

    assertEquals(expectedJexl, attributeProjection.apply(inputJexlExpression));
  }

  @Test
  void testApply_withRequestQueryParameter() {
    KeyMatch keyMatch =
        KeyMatch.newBuilder()
            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_CONTAINS)
            .setMatchKey("user_id")
            .build();
    Attribute attribute = Attribute.newBuilder().setRequestQueryParameter(keyMatch).build();

    ai.traceable.userattribution.config.service.v2.AttributeProjection inputProjection =
        ai.traceable.userattribution.config.service.v2.AttributeProjection.newBuilder()
            .setAttribute(attribute)
            .build();

    AttributeProjection attributeProjection = new AttributeProjection(inputProjection);

    String inputJexlExpression = "$a";
    String expectedJexl =
        "map:matchingValue(" + "$a.getQueryParams(), " + "predicate:contains('user_id')" + ")";

    assertEquals(expectedJexl, attributeProjection.apply(inputJexlExpression));
  }

  @Test
  void testApply_withValueProjections() {
    ValueProjection valueProjection = mock(ValueProjection.class);
    when(valueProjection.apply("$s.getRequestHeaders().get('authorization')"))
        .thenReturn("$s.getRequestHeaders().get('authorization').replaceAll('Basic ', '')");

    KeyMatch keyMatch =
        KeyMatch.newBuilder()
            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
            .setMatchKey("authorization")
            .build();
    Attribute attribute = Attribute.newBuilder().setRequestHeader(keyMatch).build();

    ai.traceable.userattribution.config.service.v2.ValueProjection mockValueProjection =
        mock(ai.traceable.userattribution.config.service.v2.ValueProjection.class);
    ai.traceable.userattribution.config.service.v2.AttributeProjection inputProjection =
        ai.traceable.userattribution.config.service.v2.AttributeProjection.newBuilder()
            .setAttribute(attribute)
            .addValueProjections(mockValueProjection)
            .build();

    try (MockedStatic<ValueProjectionFactory> mockedStatic =
        mockStatic(ValueProjectionFactory.class)) {
      mockedStatic
          .when(() -> ValueProjectionFactory.create(Mockito.any()))
          .thenReturn(valueProjection);

      AttributeProjection attributeProjection = new AttributeProjection(inputProjection);

      String inputJexlExpression = "$s";
      String result = attributeProjection.apply(inputJexlExpression);

      assertEquals("$s.getRequestHeaders().get('authorization').replaceAll('Basic ', '')", result);
    }
  }

  @Test
  void testRequestBody() {
    Attribute attribute = Attribute.newBuilder().setRequestBody(PayloadMatch.newBuilder()).build();
    ai.traceable.userattribution.config.service.v2.AttributeProjection inputProjection =
        ai.traceable.userattribution.config.service.v2.AttributeProjection.newBuilder()
            .setAttribute(attribute)
            .build();

    AttributeProjection attributeProjection = new AttributeProjection(inputProjection);

    String inputJexlExpression = "$s";
    assertEquals("$s.getRequestBody()", attributeProjection.apply(inputJexlExpression));
  }

  @Test
  void testUnsupportedAttributeType() {
    Attribute attribute =
        Attribute.newBuilder()
            .setSpanAttribute(KeyMatch.newBuilder().setMatchKey("span_id").build())
            .build();
    ai.traceable.userattribution.config.service.v2.AttributeProjection inputProjection =
        ai.traceable.userattribution.config.service.v2.AttributeProjection.newBuilder()
            .setAttribute(attribute)
            .build();

    AttributeProjection attributeProjection = new AttributeProjection(inputProjection);

    String inputJexlExpression = "$s";
    try {
      attributeProjection.apply(inputJexlExpression);
    } catch (IllegalArgumentException e) {
      assertEquals("Unsupported attribute type: SPAN_ATTRIBUTE", e.getMessage());
    }
  }
}
