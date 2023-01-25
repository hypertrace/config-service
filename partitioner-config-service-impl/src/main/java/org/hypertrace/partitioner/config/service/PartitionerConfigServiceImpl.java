package org.hypertrace.partitioner.config.service;

import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.partitioner.config.service.store.PartitionerProfilesStore;
import org.hypertrace.partitioner.config.service.v1.*;

@Slf4j
public class PartitionerConfigServiceImpl
    extends PartitionerConfigServiceGrpc.PartitionerConfigServiceImplBase {

  private final PartitionerProfilesStore partitionerProfilesStore;
  private final PartitionerConfigServiceRequestValidator validator;

  @Inject
  public PartitionerConfigServiceImpl(
      PartitionerProfilesStore partitionerProfilesStore,
      PartitionerConfigServiceRequestValidator validator) {
    this.partitionerProfilesStore = partitionerProfilesStore;
    this.validator = validator;
  }

  public void getPartitionerProfile(
      GetPartitionerProfileRequest request,
      StreamObserver<GetPartitionerProfileResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      Optional<PartitionerProfile> profile =
          partitionerProfilesStore.getPartitionerProfile(request.getProfile());
      if (profile.isPresent()) {
        responseObserver.onNext(
            GetPartitionerProfileResponse.newBuilder().setProfile(profile.get()).build());
      } else {
        responseObserver.onNext(GetPartitionerProfileResponse.newBuilder().build());
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  /** */
  public void putPartitionerConfig(
      PutPartitionerConfigRequest request,
      StreamObserver<PutPartitionerConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      partitionerProfilesStore.putPartitionerProfile(request.getProfile());
      responseObserver.onNext(PutPartitionerConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  /** */
  public void getAllPartitionerConfig(
      GetAllPartitionerConfigRequest request,
      StreamObserver<GetAllPartitionerConfigResponse> responseObserver) {
    try {
      List<PartitionerProfile> profiles = partitionerProfilesStore.getAllPartitionProfiles();
      responseObserver.onNext(
          GetAllPartitionerConfigResponse.newBuilder().addAllProfiles(profiles).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  /** */
  public void deletePartitionerConfig(
      DeletePartitionerConfigRequest request,
      StreamObserver<DeletePartitionerConfigResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      boolean deleted = partitionerProfilesStore.deletePartitionerProfile(request.getProfile());
      responseObserver.onNext(DeletePartitionerConfigResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }
}
