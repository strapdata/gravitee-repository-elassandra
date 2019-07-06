/**
 * Copyright (C) 2019 Strapdata (https://www.strapdata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.repository.elassandra.management;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.MembershipRepository;
import io.gravitee.repository.management.model.Membership;
import io.gravitee.repository.management.model.MembershipReferenceType;
import io.gravitee.repository.management.model.RoleScope;

/**
 * @author LeansysTeam (leansys dot fr)
 */
@Repository
public class ElassandraMembershipRepository extends ElassandraCrud<Membership, String> implements MembershipRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraMembershipRepository.class);

    public ElassandraMembershipRepository() throws IOException {
        super("memberships",
                new String[] {"user_id","reference_type","reference_id", "roles", "created_at", "updated_at"},
                "memberships",
                Settings.builder().put("synchronous_refresh", true),
                XContentFactory.jsonBuilder()
                        .startObject()
                          .startObject("properties")
                            .startObject("user_id")
                                .field("type", "keyword")
                                .field("cql_collection", "singleton")
                                .field("cql_primary_key_order", 0)
                                .field("cql_partition_key", true)
                            .endObject()
                            .startObject("reference_type")
                                .field("type", "keyword")
                                .field("cql_collection", "singleton")
                                .field("cql_primary_key_order", 1)
                            .endObject()
                            .startObject("reference_id")
                                .field("type", "keyword")
                                .field("cql_collection", "singleton")
                                .field("cql_primary_key_order", 2)
                            .endObject()
                            .startObject("roles").field("type", "keyword").field("cql_collection", "set").endObject()
                            .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                            .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Membership t) {
        return new Object[] {
                t.getUserId(),
                t.getReferenceType() == null ? null : t.getReferenceType().toString(),
                t.getReferenceId(),
                convertRolesToStrings(t),
                t.getCreatedAt(),
                t.getUpdatedAt()
        };
    }

    @Override
    public Membership fromRow(Row row) {
        if (row != null) {
            final Membership membership = new Membership();
            membership.setUserId(row.getString("user_id"));
            membership.setReferenceId(row.getString("reference_id"));
            membership.setReferenceType(MembershipReferenceType.valueOf(row.getString("reference_type").toUpperCase()));
            Set<String> rolesAsString = row.getSet("roles", String.class);
            Map<Integer, String> roles = new HashMap<>(rolesAsString.size());
            for (String roleAsString : rolesAsString) {
                String[] role = convertTypeToRole(roleAsString);
                roles.put(Integer.valueOf(role[0]), role[1]);
            }
            membership.setRoles(roles);
            membership.setCreatedAt(row.getTimestamp("created_at"));
            membership.setUpdatedAt(row.getTimestamp("updated_at"));
            return membership;
        }
        return null;
    }

    @Override
    public Set<Membership> findByIds(String userId, MembershipReferenceType referenceType, Set<String> referenceIds) throws TechnicalException {
        LOGGER.debug("Find Membership by IDs [{}]-[{}]-[{}]", userId, referenceType, referenceIds);

        if (userId == null) {
            return Collections.emptySet();
        }

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("user_id", userId));
        if (referenceIds != null && !referenceIds.isEmpty())
            queryBuilder.filter(QueryBuilders.termsQuery("reference_id", referenceIds));
        if (referenceType != null)
            queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType.toString()));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByReferenceAndRole(MembershipReferenceType referenceType, String referenceId, RoleScope roleScope, String roleName) throws TechnicalException {
        String membershipType = convertRoleToType(roleScope, roleName);
        LOGGER.debug("Find Membership by Reference & MembershipType [{}]-[{}]-[{}]", referenceType, referenceId, membershipType);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (membershipType != null)
            queryBuilder.filter(QueryBuilders.termsQuery("roles", membershipType));
        if (referenceId != null)
            queryBuilder.filter(QueryBuilders.termQuery("reference_id", referenceId));
        if (referenceType != null)
            queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByReferencesAndRole(MembershipReferenceType referenceType, List<String> referenceIds, RoleScope roleScope, String roleName) throws TechnicalException {
        String membershipType = convertRoleToType(roleScope, roleName);
        LOGGER.debug("Find Membership by References & MembershipType [{}]-[{}]", referenceType, membershipType);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (membershipType != null)
            queryBuilder.filter(QueryBuilders.termsQuery("roles", membershipType));
        if (referenceIds != null && !referenceIds.isEmpty())
            queryBuilder.filter(QueryBuilders.termsQuery("reference_id", referenceIds));
        if (referenceType != null)
            queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByUserAndReferenceType(String userId, MembershipReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Find Membership by User & Reference [{}]-[{}]", userId, referenceType);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (userId != null)
            queryBuilder.filter(QueryBuilders.termQuery("user_id", userId));
        if (referenceType != null)
            queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByUserAndReferenceTypeAndRole(String userId, MembershipReferenceType referenceType, RoleScope roleScope, String roleName) throws TechnicalException {
        String membershipType = convertRoleToType(roleScope, roleName);
        LOGGER.debug("Find Membership by User, Reference, MembershipType, RoleScope, roleName [{}]-[{}]-[{}]", userId, referenceType, membershipType);

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("roles", membershipType));
        if (userId != null)
            queryBuilder.filter(QueryBuilders.termQuery("user_id", userId));
        if (referenceType != null)
            queryBuilder.filter(QueryBuilders.termQuery("reference_type", referenceType));

        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }


    private Set<String> convertRolesToStrings(Membership membership) {
        if (membership.getRoles() != null) {
            Set<String> roles = new HashSet<>(membership.getRoles().size());
            for (Map.Entry<Integer, String> roleEntry : membership.getRoles().entrySet()) {
                roles.add(convertRoleToType(roleEntry.getKey(), roleEntry.getValue()));
            }
            return roles;
        }
        return Collections.emptySet();
    }

    private String convertRoleToType(RoleScope roleScope, String roleName) {
        if (roleName == null) {
            return null;
        }
        return convertRoleToType(roleScope.getId(), roleName);
    }

    private String convertRoleToType(int roleScope, String roleName) {
        return roleScope + ":" + roleName;
    }

    private String[] convertTypeToRole(String type) {
        if(type == null) {
            return null;
        }
        String[] role = type.split(":");
        if (role .length != 2) {
            return null;
        }
        return role;
    }

    @Override
    public Set<Membership> findByRole(RoleScope roleScope, String roleName) throws TechnicalException {
        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("roles", convertRoleToType(roleScope, roleName)))
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("ElassandraMembershipRepository.query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByUser(String userId) throws TechnicalException {
        LOGGER.debug("findByUser({})", userId);
        try {
            if (userId == null) {
                return Collections.EMPTY_SET;
            }
            String esQuery = new SearchSourceBuilder()
                    .query(QueryBuilders.termQuery("user_id", userId))
                    .toString(ToXContent.EMPTY_PARAMS);
            final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
            return resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());
        } catch (final Exception ex) {
            LOGGER.error("Failed to find membership by user ", ex);
            throw new TechnicalException("Failed to find by user ", ex);
        }
    }

    @Override
    public void delete(Membership membership) throws TechnicalException {
        if (membership == null)
            throw new IllegalStateException("null primary key");
        if (membership.getUserId() == null || membership.getReferenceType() == null || membership.getReferenceId() == null)
            throw new IllegalStateException("One primary key is null");
        LOGGER.debug("delete({}, {}, {})", membership.getUserId(), membership.getReferenceType().toString(), membership.getReferenceId());
        session.execute(deleteStmt.bind(membership.getUserId(), membership.getReferenceType().toString(), membership.getReferenceId()));
    }

    @Override
    public Optional<Membership> findById(String userId, MembershipReferenceType referenceType, String referenceId)
            throws TechnicalException {
        if (userId == null || referenceType == null || referenceId == null)
            return Optional.empty();
        LOGGER.debug("findById({}, {}, {})", userId, referenceType, referenceId);
        return Optional.ofNullable(fromRow(session.execute(selectStmt.bind(userId, referenceType == null ? null : referenceType.toString(), referenceId)).one()));
    }
}
