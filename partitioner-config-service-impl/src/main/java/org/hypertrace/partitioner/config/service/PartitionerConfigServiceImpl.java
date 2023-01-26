package org.hypertrace.partitioner.config.service;

import com.google.inject.Inject;
import io.grpc.Status;
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
          partitionerProfilesStore.getPartitionerProfile(request.getProfileName());
      if (profile.isPresent()) {
        responseObserver.onNext(
            GetPartitionerProfileResponse.newBuilder().setProfile(profile.get()).build());
      } else {
        throw Status.NOT_FOUND
            .withDescription(
                String.format("Partitioner profile for {} not found", request.getProfileName()))
            .asRuntimeException();
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get profile failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void putPartitionerProfiles(
      PutPartitionerProfilesRequest request,
      StreamObserver<PutPartitionerProfilesResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      partitionerProfilesStore.putPartitionerProfiles(request.getProfilesList());
      responseObserver.onNext(PutPartitionerProfilesResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Put profiles failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void getPartitionerProfiles(
      GetPartitionerProfilesRequest request,
      StreamObserver<GetPartitionerProfilesResponse> responseObserver) {
    try {
      List<PartitionerProfile> profiles = partitionerProfilesStore.getAllPartitionProfiles();
      responseObserver.onNext(
          GetPartitionerProfilesResponse.newBuilder().addAllProfiles(profiles).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Get profiles failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }

  public void deletePartitionerProfiles(
      DeletePartitionerProfilesRequest request,
      StreamObserver<DeletePartitionerProfilesResponse> responseObserver) {
    try {
      validator.validateOrThrow(request);
      partitionerProfilesStore.deletePartitionerProfiles(request.getProfileList());
      responseObserver.onNext(DeletePartitionerProfilesResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Delete profiles failed for request:{}", request, e);
      responseObserver.onError(e);
    }
  }
}
