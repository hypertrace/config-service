package ai.traceable.data.handling.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.config.utils.ObjectDiffer;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.handling.config.service.store.DataHandlingRuleStore;
import ai.traceable.data.handling.config.service.utils.DataHandlingRuleGenerator;
import ai.traceable.data.handling.config.service.utils.DataHandlingRuleRankCalculator;
import ai.traceable.data.handling.config.service.v1.CreateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.DataHandlingConfigServiceGrpc;
import ai.traceable.data.handling.config.service.v1.DataHandlingConfigServiceGrpc.DataHandlingConfigServiceBlockingStub;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleAction;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleCondition;
import ai.traceable.data.handling.config.service.v1.DataHandlingRuleData;
import ai.traceable.data.handling.config.service.v1.DeleteDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.GetDataHandlingRulesRequest;
import ai.traceable.data.handling.config.service.v1.RankDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.v1.UpdateDataHandlingRuleRequest;
import ai.traceable.data.handling.config.service.validation.DataHandlingConfigRequestValidator;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataHandlingConfigServiceImplTest {
  private static final DataHandlingRuleData RULE_DATA =
      DataHandlingRuleData.newBuilder()
          .setName("test-rule")
          .addConditions(DataHandlingRuleCondition.getDefaultInstance())
          .addActions(DataHandlingRuleAction.getDefaultInstance())
          .build();

  DataHandlingConfigServiceBlockingStub dataHandlingConfigStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());

    this.mockGenericConfigService
        .addService(
            new DataHandlingConfigServiceImpl(
                new DataHandlingConfigRequestValidator(),
                new DataHandlingRuleStore(genericStub, new DataHandlingRuleRankCalculator()),
                new ObjectDiffer(),
                new DataHandlingRuleRankCalculator(),
                new DataHandlingRuleGenerator(new UuidGenerator())))
        .start();

    this.dataHandlingConfigStub =
        DataHandlingConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void lotsOfCrud() {
    DataHandlingRule firstCreated =
        this.dataHandlingConfigStub
            .createDataHandlingRule(
                CreateDataHandlingRuleRequest.newBuilder().setData(RULE_DATA).build())
            .getRules(0);

    DataHandlingRule firstUpdated =
        this.dataHandlingConfigStub
            .updateDataHandlingRule(
                UpdateDataHandlingRuleRequest.newBuilder()
                    .setId(firstCreated.getId())
                    .setData(RULE_DATA.toBuilder().setName("first updated"))
                    .build())
            .getRule();

    assertEquals(1, firstUpdated.getRank());

    List<DataHandlingRule> afterSecondCreate =
        this.dataHandlingConfigStub
            .createDataHandlingRule(
                CreateDataHandlingRuleRequest.newBuilder()
                    .setData(RULE_DATA.toBuilder().setName("second"))
                    .build())
            .getRulesList();
    assertEquals(firstUpdated, afterSecondCreate.get(0));
    DataHandlingRule secondCreated = afterSecondCreate.get(1);
    assertEquals(2, secondCreated.getRank());
    assertEquals("second", secondCreated.getData().getName());

    assertEquals(
        List.of(firstUpdated, secondCreated),
        this.dataHandlingConfigStub
            .getDataHandlingRules(GetDataHandlingRulesRequest.getDefaultInstance())
            .getRulesList());

    assertEquals(
        List.of(withRank(secondCreated, 1), withRank(firstUpdated, 2)),
        this.dataHandlingConfigStub
            .rankDataHandlingRule(
                RankDataHandlingRuleRequest.newBuilder()
                    .setRuleIdToUpdate(firstUpdated.getId())
                    .setPrecedingRuleId(secondCreated.getId())
                    .build())
            .getRulesList());

    assertEquals(
        List.of(withRank(firstUpdated, 1), withRank(secondCreated, 2)),
        this.dataHandlingConfigStub
            .rankDataHandlingRule(
                RankDataHandlingRuleRequest.newBuilder()
                    .setRuleIdToUpdate(firstUpdated.getId())
                    .build())
            .getRulesList());

    assertEquals(
        List.of(withRank(secondCreated, 1)),
        this.dataHandlingConfigStub
            .deleteDataHandlingRule(
                DeleteDataHandlingRuleRequest.newBuilder().setId(firstUpdated.getId()).build())
            .getRulesList());

    assertEquals(
        List.of(withRank(secondCreated, 1)),
        this.dataHandlingConfigStub
            .getDataHandlingRules(GetDataHandlingRulesRequest.getDefaultInstance())
            .getRulesList());
  }

  private DataHandlingRule withRank(DataHandlingRule rule, int rank) {
    return rule.toBuilder().setRank(rank).build();
  }
}
