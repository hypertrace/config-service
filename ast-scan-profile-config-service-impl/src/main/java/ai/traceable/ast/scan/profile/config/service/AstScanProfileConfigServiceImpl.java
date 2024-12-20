package ai.traceable.ast.scan.profile.config.service;

import ai.traceable.ast.scan.profile.config.service.store.AstScanProfileConfigStore;
import ai.traceable.ast.scan.profile.config.service.v1.CreateScanProfileRequest;
import ai.traceable.ast.scan.profile.config.service.v1.CreateScanProfileResponse;
import ai.traceable.ast.scan.profile.config.service.v1.DeleteScanProfilesRequest;
import ai.traceable.ast.scan.profile.config.service.v1.DeleteScanProfilesResponse;
import ai.traceable.ast.scan.profile.config.service.v1.GetScanProfilesRequest;
import ai.traceable.ast.scan.profile.config.service.v1.GetScanProfilesResponse;
import ai.traceable.ast.scan.profile.config.service.v1.ScanProfile;
import ai.traceable.ast.scan.profile.config.service.v1.ScanProfileConfigServiceGrpc;
import ai.traceable.ast.scan.profile.config.service.v1.UpdateScanProfileRequest;
import ai.traceable.ast.scan.profile.config.service.v1.UpdateScanProfileResponse;
import ai.traceable.ast.scan.profile.config.service.v1.UpsertScanProfile;
import ai.traceable.ast.scan.profile.config.service.validator.AstScanProfileConfigRequestValidator;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AstScanProfileConfigServiceImpl
    extends ScanProfileConfigServiceGrpc.ScanProfileConfigServiceImplBase {
  private final AstScanProfileConfigStore configStore;
  private final AstScanProfileConfigRequestValidator requestValidator;

  @Inject
  AstScanProfileConfigServiceImpl(
      AstScanProfileConfigStore configStore,
      AstScanProfileConfigRequestValidator requestValidator) {
    this.configStore = configStore;
    this.requestValidator = requestValidator;
  }

  @Override
  public void createScanProfile(
      CreateScanProfileRequest request,
      StreamObserver<CreateScanProfileResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);

      UpsertScanProfile createScanProfile = request.getCreateScanProfile();

      ScanProfile scanProfile =
          ScanProfile.newBuilder()
              .setName(request.getName())
              .setDescription(createScanProfile.getDescription())
              .setProfileConfiguration(createScanProfile.getProfileConfiguration())
              .build();

      ContextualConfigObject<ScanProfile> contextualConfigObject =
          this.configStore.upsertObject(requestContext, scanProfile);

      responseObserver.onNext(
          CreateScanProfileResponse.newBuilder()
              .setScanProfile(contextualConfigObject.getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error creating ast scan profile for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateScanProfile(
      UpdateScanProfileRequest request,
      StreamObserver<UpdateScanProfileResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);
      UpsertScanProfile updateScanProfile = request.getUpdateScanProfile();
      ScanProfile scanProfile =
          ScanProfile.newBuilder()
              .setName(request.getName())
              .setDescription(updateScanProfile.getDescription())
              .setProfileConfiguration(updateScanProfile.getProfileConfiguration())
              .build();

      ContextualConfigObject<ScanProfile> contextualConfigObject =
          this.configStore.upsertObject(requestContext, scanProfile);

      responseObserver.onNext(
          UpdateScanProfileResponse.newBuilder()
              .setScanProfile(contextualConfigObject.getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error updating ast scan profile for request: {}", request, exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getScanProfiles(
      GetScanProfilesRequest request, StreamObserver<GetScanProfilesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);
      List<String> profileNames = request.getFilter().getScanProfileNamesList();
      if (profileNames.isEmpty()) {
        responseObserver.onNext(
            GetScanProfilesResponse.newBuilder()
                .addAllScanProfiles(configStore.getAllConfigData(requestContext))
                .build());
      } else {
        List<ScanProfile> scanProfiles = new ArrayList<>();
        profileNames.forEach(
            name -> {
              Optional<ScanProfile> maybeScanProfile = configStore.getData(requestContext, name);
              if (maybeScanProfile.isPresent()) {
                scanProfiles.add(maybeScanProfile.get());
              } else {
                log.error(
                    "Not able to get ast scan profile with name: "
                        + name
                        + " for tenant:"
                        + requestContext.getTenantId());
              }
            });
        responseObserver.onNext(
            GetScanProfilesResponse.newBuilder().addAllScanProfiles(scanProfiles).build());
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Error getting ast scan profiles for request: {}", request, e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteScanProfiles(
      DeleteScanProfilesRequest request,
      StreamObserver<DeleteScanProfilesResponse> responseObserver) {
    try {
      RequestContext requestContext = RequestContext.CURRENT.get();
      this.requestValidator.validateOrThrow(requestContext, request);

      this.configStore.deleteObjects(requestContext, request.getFilter().getScanProfileNamesList());

      responseObserver.onNext(DeleteScanProfilesResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error("Error deleting ast scan profile for request: {}", request, exception);
      responseObserver.onError(exception);
    }
  }
}
