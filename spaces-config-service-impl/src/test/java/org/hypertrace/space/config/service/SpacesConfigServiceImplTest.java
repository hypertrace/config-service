package org.hypertrace.space.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;
import org.hypertrace.config.service.MockGenericConfigService;
import org.hypertrace.spaces.config.service.v1.AttributeValueData;
import org.hypertrace.spaces.config.service.v1.CreateRuleRequest;
import org.hypertrace.spaces.config.service.v1.DeleteRuleRequest;
import org.hypertrace.spaces.config.service.v1.GetRulesRequest;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc.SpacesConfigServiceBlockingStub;
import org.hypertrace.spaces.config.service.v1.UpdateRuleRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpacesConfigServiceImplTest {
  SpacesConfigServiceBlockingStub spacesStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGetAll().mockDelete();

    this.mockGenericConfigService
        .addService(new SpacesConfigServiceImpl(this.mockGenericConfigService.channel()))
        .start();

    this.spacesStub =
        SpacesConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void createReadUpdateReadDelete() {

    AttributeValueData attributeValueData1 =
        AttributeValueData.newBuilder()
            .setAttributeScope("attrScope")
            .setAttributeKey("attrKey1")
            .build();

    AttributeValueData attributeValueData2 =
        AttributeValueData.newBuilder()
            .setAttributeScope("attrScope")
            .setAttributeKey("attrKey2")
            .build();

    SpaceConfigRule createdRule1 =
        this.spacesStub
            .createRule(
                CreateRuleRequest.newBuilder().setAttributeValueData(attributeValueData1).build())
            .getRule();

    assertEquals(attributeValueData1, createdRule1.getAttributeValueData());
    assertFalse(createdRule1.getId().isEmpty());

    SpaceConfigRule createdRule2 =
        this.spacesStub
            .createRule(
                CreateRuleRequest.newBuilder().setAttributeValueData(attributeValueData2).build())
            .getRule();

    assertIterableEquals(
        List.of(createdRule1, createdRule2),
        this.spacesStub.getRules(GetRulesRequest.getDefaultInstance()).getRulesList());

    SpaceConfigRule ruleToUpdate =
        createdRule1.toBuilder()
            .setAttributeValueData(
                attributeValueData1.toBuilder().setAttributeKey("updatedAttrKey1"))
            .build();

    SpaceConfigRule updatedRule1 =
        this.spacesStub
            .updateRule(UpdateRuleRequest.newBuilder().setUpdatedRule(ruleToUpdate).build())
            .getRule();

    assertEquals(ruleToUpdate, updatedRule1);

    assertIterableEquals(
        List.of(updatedRule1, createdRule2),
        this.spacesStub.getRules(GetRulesRequest.getDefaultInstance()).getRulesList());

    this.spacesStub.deleteRule(DeleteRuleRequest.newBuilder().setId(createdRule2.getId()).build());

    assertIterableEquals(
        List.of(updatedRule1),
        this.spacesStub.getRules(GetRulesRequest.getDefaultInstance()).getRulesList());
  }
}
