package org.hypertrace.partitioner.config.service.store;

import com.google.common.collect.ImmutableList;
import java.util.*;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.*;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.partitioner.config.service.v1.PartitionerProfile;

public class PartitionerProfilesDocumentStore implements PartitionerProfilesStore {
  public static final String PARTITIONER_PROFILES = "partitioner_profiles";
  private static final String PARTITIONER_PROFILE_NAME_FIELD = "name";

  private final Collection collection;

  public PartitionerProfilesDocumentStore(Datastore datastore) {
    this.collection = datastore.getCollection(PARTITIONER_PROFILES);
  }

  @Override
  public Optional<PartitionerProfile> getPartitionerProfile(String profile) throws Exception {
    Query query = new Query();
    query.setFilter(new Filter(Filter.Op.EQ, PARTITIONER_PROFILE_NAME_FIELD, profile));
    try (CloseableIterator<Document> it = collection.search(query)) {
      List<Document> list = ImmutableList.copyOf(it);
      if (list.size() == 1) {
        return Optional.of(PartitionerProfileDocument.fromJson(list.get(0).toJson()));
      } else if (list.size() > 1) {
        throw new IllegalStateException("More than 1 PartitionerProfile documents returned");
      }
    }
    return Optional.empty();
  }

  @Override
  public List<PartitionerProfile> getAllPartitionProfiles() throws Exception {
    try (CloseableIterator<Document> it = collection.search(new Query())) {
      List<Document> list = ImmutableList.copyOf(it);
      return list.stream()
          .map(document -> PartitionerProfileDocument.fromJson(document.toJson()))
          .collect(Collectors.toList());
    }
  }

  @Override
  public Optional<PartitionerProfile> putPartitionerProfile(PartitionerProfile partitionerProfile)
      throws Exception {
    Document upsertedDoc =
        this.collection.upsertAndReturn(
            new PartitionerProfileKey(partitionerProfile.getName()),
            new PartitionerProfileDocument(partitionerProfile));
    return Optional.of(PartitionerProfileDocument.fromJson(upsertedDoc.toJson()));
  }

  @Override
  public boolean deletePartitionerProfile(String profile) throws Exception {
    return this.collection.delete(new PartitionerProfileKey(profile));
  }
}
