package ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_ALLOWED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SNOOZED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SUSPENDED;
import static ai.traceable.platform.actor.v1.StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.config.ActorServiceConfig;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.platform.actor.v1.GetActorsByStatusRequest;
import ai.traceable.platform.actor.v1.GetActorsByStatusResponse;
import ai.traceable.platform.actor.v1.Status;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ActorBasedDataFetcherImpl implements ActorBasedDataFetcher {
  private final ActorServiceBlockingStub actorServiceStub;
  private final Duration callTimeout;
  private final BlockingRulesUtils blockingRulesUtils;

  @Inject
  public ActorBasedDataFetcherImpl(
      ActorServiceBlockingStub actorServiceStub,
      ActorServiceConfig actorServiceConfig,
      BlockingRulesUtils blockingRulesUtils) {
    this.actorServiceStub = actorServiceStub;
    this.callTimeout = actorServiceConfig.getCallTimeoutDuration();
    this.blockingRulesUtils = blockingRulesUtils;
  }

  @Override
  public List<BlockingDetails> getThreatActorBasedIpViolation() {
    return getActiveActors(List.of(STATUS_ALWAYS_DENIED, STATUS_SUSPENDED)).stream()
        .filter(actor -> actor.getStatusChangeSource() != STATUS_CHANGE_SOURCE_RATE_LIMIT)
        .filter(actor -> !actor.getIpAddressesList().isEmpty())
        .filter(actor -> blockingRulesUtils.isRuleActive(actor.getStatusExpiryTimestamp()))
        .map(
            actor ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_THREAT_ACTOR)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                    .setInfo(
                        ViolationInfoEncoder.getEncodedThreatActorViolationInfo(
                            actor.getEntityId()))
                    .setExpirationTimestamp(actor.getStatusExpiryTimestamp())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            actor.getStatusExpiryTimestamp(), BLOCKING_RULE_TYPE_BLOCK))
                    .setIpDetails(
                        IpDetails.newBuilder()
                            .addAllIpAddresses(actor.getIpAddressesList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<BlockingDetails> getThreatActorBasedIpExemption() {
    return getActiveActors(List.of(STATUS_ALWAYS_ALLOWED, STATUS_SNOOZED)).stream()
        .filter(actor -> actor.getStatusChangeSource() != STATUS_CHANGE_SOURCE_RATE_LIMIT)
        .filter(actor -> !actor.getIpAddressesList().isEmpty())
        .filter(actor -> blockingRulesUtils.isRuleActive(actor.getStatusExpiryTimestamp()))
        .map(
            actor ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_THREAT_ACTOR)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_ALLOW)
                    .setInfo(
                        ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo(
                            actor.getEntityId()))
                    .setExpirationTimestamp(actor.getStatusExpiryTimestamp())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            actor.getStatusExpiryTimestamp(), BLOCKING_RULE_TYPE_ALLOW))
                    .setIpDetails(
                        IpDetails.newBuilder()
                            .addAllIpAddresses(actor.getIpAddressesList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<BlockingDetails> getRateLimitBasedIpViolation() {
    return getActiveActors(List.of(STATUS_ALWAYS_DENIED, STATUS_SUSPENDED)).stream()
        .filter(actor -> actor.getStatusChangeSource() == STATUS_CHANGE_SOURCE_RATE_LIMIT)
        .filter(actor -> !actor.getIpAddressesList().isEmpty())
        .filter(actor -> blockingRulesUtils.isRuleActive(actor.getStatusExpiryTimestamp()))
        .map(
            actor ->
                BlockingDetails.newBuilder()
                    .setCategory(BLOCKING_CATEGORY_RATE_LIMIT)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                    .setInfo(
                        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
                            actor.getEntityId(),
                            actor.getStatusChangeDetails().getRateLimitDetails().getRuleId(),
                            actor.getStatusChangeDetails().getRateLimitDetails().getRuleName()))
                    .setExpirationTimestamp(actor.getStatusExpiryTimestamp())
                    .setStatus(
                        blockingRulesUtils.generateBlockingStatus(
                            actor.getStatusExpiryTimestamp(), BLOCKING_RULE_TYPE_BLOCK))
                    .setIpDetails(
                        IpDetails.newBuilder()
                            .addAllIpAddresses(actor.getIpAddressesList())
                            .build())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  // TODO: add cache for actor-service
  private List<Actor> getActiveActors(List<Status> filterStatus) {
    GetActorsByStatusResponse actorsByStatusResponse =
        actorServiceStub
            .withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)
            .getActorsByStatus(
                GetActorsByStatusRequest.newBuilder().addAllStatus(filterStatus).build());
    return actorsByStatusResponse.getActorsList();
  }
}
