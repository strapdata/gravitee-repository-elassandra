/**
 * This file is part of Gravitee.io APIM - API Management - Repository for Elassandra.
 *
 * Gravitee.io APIM - API Management - Repository for Elassandra is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gravitee.io APIM - API Management - Repository for Elassandra is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Gravitee.io APIM - API Management - Repository for Elassandra.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.gravitee.repository.elassandra.management;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
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

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.MetadataRepository;
import io.gravitee.repository.management.model.Membership;
import io.gravitee.repository.management.model.MembershipReferenceType;
import io.gravitee.repository.management.model.Metadata;
import io.gravitee.repository.management.model.MetadataFormat;
import io.gravitee.repository.management.model.MetadataReferenceType;

/**
 * @author vroyer
 */
@Repository
public class ElassandraMetadataRepository extends ElassandraCrud<Metadata, Object[]> implements MetadataRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraMetadataRepository.class);

    public ElassandraMetadataRepository() throws IOException {
        super("metadata",
                new String[]{"key", "reference_type", "reference_id", "name", "format", "value", "created_at", "updated_at"},
                "metadata",
                Settings.builder().put("synchronous_refresh", true),
                XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("key")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 0)
                            .field("cql_partition_key", true)
                        .endObject()
                            .startObject("reference_type")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 1)
                            .field("cql_partition_key", false)
                        .endObject()
                        .startObject("reference_id")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 2)
                            .field("cql_partition_key", false)
                        .endObject()
                        .startObject("name").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("format").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("value").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("updated_at").field("type", "date").field("cql_collection", "singleton").field("index", false).endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Metadata metadata) {
        return new Object[]{
            metadata.getKey(),
            metadata.getReferenceType() == null ? null : metadata.getReferenceType().name(),
            metadata.getReferenceId(),
            metadata.getName(),
            metadata.getFormat() == null ? null : metadata.getFormat().name(),
            metadata.getValue(),
            metadata.getCreatedAt(),
            metadata.getUpdatedAt()
            };
    }

    // CREATE TABLE workflows(id text primary key, name text, notifier text, config text, hooks list<text>, reference_type text, reference_id text, created_at timestamp, updatedAt timestamp, use_system_proxy boolean)
    @Override
    public Metadata fromRow(Row row) {
        if (row != null) {
            final Metadata metadata = new Metadata();
            metadata.setKey(row.getString("key"));
            metadata.setName(row.getString("name"));
            metadata.setValue(row.getString("value"));
            metadata.setFormat(MetadataFormat.valueOf(row.getString("format")));
            metadata.setReferenceId(row.getString("reference_id"));
            metadata.setReferenceType(MetadataReferenceType.valueOf(row.getString("reference_type")));
            metadata.setCreatedAt(row.getTimestamp("created_at"));
            metadata.setUpdatedAt(row.getTimestamp("updated_at"));
            return metadata;
        }
        return null;
    }


    @Override
    public List<Metadata> findByKeyAndReferenceType(final String key, final MetadataReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Find all metadata by key and reference type");

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("key", key))
                .filter(QueryBuilders.termQuery("reference_type", referenceType));
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    @Override
    public List<Metadata> findByReferenceType(final MetadataReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Find all metadata by reference type");
        String esQuery = new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("reference_type", referenceType))
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    @Override
    public List<Metadata> findByReferenceTypeAndReferenceId(final MetadataReferenceType referenceType, final String referenceId) throws TechnicalException {
        LOGGER.debug("Find all metadata by reference type and reference id");
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("reference_id", referenceId))
                .filter(QueryBuilders.termQuery("reference_type", referenceType));
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return resultSet.all().stream().map(this::fromRow).collect(Collectors.toList());
    }

    @Override
    public void delete(String key, String referenceId, MetadataReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("delete({}, {}, {})", key, referenceType.toString(), referenceId);
        session.execute(deleteStmt.bind(key, referenceType == null ? null : referenceType.toString(), referenceId));
    }

    @Override
    public Optional<Metadata> findById(String key, String referenceId, MetadataReferenceType referenceType)
            throws TechnicalException {
        LOGGER.debug("findById({}, {}, {})", key, referenceType, referenceId);
        return Optional.ofNullable(fromRow(session.execute(selectStmt.bind(key, referenceType == null ? null : referenceType.toString(), referenceId)).one()));
    }
}
