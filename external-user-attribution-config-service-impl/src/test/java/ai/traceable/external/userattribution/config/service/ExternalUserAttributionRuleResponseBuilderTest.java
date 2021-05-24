package ai.traceable.external.userattribution.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import ai.traceable.external.userattribution.config.service.v1.ExternalUserAttributionRules;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesRequest;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesRequest.OnlyIfChangedFilter;
import ai.traceable.external.userattribution.config.service.v1.GetExternalUserAttributionRulesResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExternalUserAttributionRuleResponseBuilderTest {

  @Mock HashGenerator mockHashGenerator;
  ExternalUserAttributionRules mockRules = ExternalUserAttributionRules.newBuilder().build();
  GetExternalUserAttributionRulesRequest mockRequest =
      GetExternalUserAttributionRulesRequest.newBuilder()
          .setFilter(OnlyIfChangedFilter.newBuilder().setPreviousHash("previous-hash"))
          .build();

  ExternalUserAttributionRuleResponseBuilder responseBuilder;

  @BeforeEach
  void beforeEach() {
    this.responseBuilder = new ExternalUserAttributionRuleResponseBuilder(this.mockHashGenerator);
  }

  @Test
  void emptyRulesIfMatchHash() {
    when(this.mockHashGenerator.generateHash(mockRules))
        .thenReturn(mockRequest.getFilter().getPreviousHash());

    GetExternalUserAttributionRulesResponse response =
        this.responseBuilder.buildResponse(mockRequest, mockRules);
    assertFalse(response.hasExternalUserAttributionRules());
    assertEquals(mockRequest.getFilter().getPreviousHash(), response.getHash());
  }

  @Test
  void returnsRulesIfDifferentHash() {
    String differentHash = "different-hash";
    when(this.mockHashGenerator.generateHash(mockRules)).thenReturn(differentHash);

    GetExternalUserAttributionRulesResponse response =
        this.responseBuilder.buildResponse(mockRequest, mockRules);
    assertSame(mockRules, response.getExternalUserAttributionRules());
    assertEquals(differentHash, response.getHash());
  }
}
