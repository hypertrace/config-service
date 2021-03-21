package ai.traceable.blocking.config.service.regions;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.CreateRegionRuleResponse;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetDetailedRegionsResponse;
import ai.traceable.region.config.service.v1.IpRange;
import ai.traceable.region.config.service.v1.IpV4Range;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceImplBase;
import ai.traceable.region.config.service.v1.RegionRule;
import com.google.common.collect.Lists;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

class MockRegionConfigService extends RegionConfigServiceImplBase {
  private Server grpcServer;
  private final InProcessServerBuilder serverBuilder;
  private final ManagedChannel configChannel;

  private final Map<String, RegionRule> rules = new LinkedHashMap<>();

  public MockRegionConfigService() {
    String uniqueName = InProcessServerBuilder.generateName();
    this.configChannel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();
    this.serverBuilder =
        InProcessServerBuilder.forName(uniqueName).directExecutor().addService(this);
  }

  public void start() {
    try {
      this.grpcServer = serverBuilder.build().start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Channel channel() {
    return this.configChannel;
  }

  public void shutdown() {
    this.rules.clear();
    this.grpcServer.shutdownNow();
    this.configChannel.shutdownNow();
  }

  @Override
  public void getDetailedRegions(
      GetDetailedRegionsRequest request,
      StreamObserver<GetDetailedRegionsResponse> responseObserver) {
    responseObserver.onNext(
        GetDetailedRegionsResponse.newBuilder().addAllRegion(mockDetailedRegions()).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getAllRegionRules(
      GetAllRegionRulesRequest request,
      StreamObserver<GetAllRegionRulesResponse> responseObserver) {
    GetAllRegionRulesResponse response =
        rules.values().stream()
            .collect(
                Collectors.collectingAndThen(
                    Collectors.toList(),
                    list ->
                        GetAllRegionRulesResponse.newBuilder()
                            .addAllRule(Lists.reverse(list))
                            .build()));

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void createRegionRule(
      CreateRegionRuleRequest request, StreamObserver<CreateRegionRuleResponse> responseObserver) {
    String id = generateId();
    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId(id)
            .setActionType(request.getActionType())
            .setName(request.getName())
            .addAllRegionId(request.getRegionIdList())
            .setExpirationMillis(request.getExpirationMillis())
            .build();
    rules.put(id, regionRule);
    responseObserver.onNext(CreateRegionRuleResponse.newBuilder().setRule(regionRule).build());
    responseObserver.onCompleted();
  }

  private List<DetailedRegion> mockDetailedRegions() {
    return List.of(
        DetailedRegion.newBuilder()
            .setId(generateId())
            .setName("region-1")
            .addIpRange(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(123L).setEndIp(789L).build())
                    .build())
            .build(),
        DetailedRegion.newBuilder()
            .setId(generateId())
            .setName("region-2")
            .addIpRange(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(123L).setEndIp(234L).build())
                    .build())
            .addIpRange(
                IpRange.newBuilder()
                    .setIpv4Range(IpV4Range.newBuilder().setStartIp(234L).setEndIp(456L).build())
                    .build())
            .build());
  }

  private String generateId() {
    return UUID.randomUUID().toString();
  }
}
