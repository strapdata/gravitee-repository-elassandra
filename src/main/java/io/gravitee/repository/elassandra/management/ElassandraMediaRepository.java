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
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Optional;

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
import com.datastax.driver.core.utils.Bytes;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.media.api.MediaRepository;
import io.gravitee.repository.media.model.Media;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraMediaRepository extends ElassandraCrud<Media, String> implements MediaRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraDictionaryRepository.class);

    public ElassandraMediaRepository() throws IOException {
        super("media",
                new String[] {"id","type","sub_type", "file_name", "hash", "size", "data", "api", "created_at"},
                "media",
                Settings.builder().put("synchronous_refresh", true),
                XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("id")
                            .field("type", "keyword")
                            .field("cql_collection", "singleton")
                            .field("cql_primary_key_order", 0)
                            .field("cql_partition_key", true)
                        .endObject()
                        .startObject("type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("sub_type").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("file_name").field("type", "keyword").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("hash").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("size").field("type", "long").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("data").field("type", "binary").field("cql_collection", "singleton").field("index", false).endObject()
                        .startObject("api").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("created_at").field("type", "date").field("cql_collection", "singleton").endObject()
                    .endObject()
                .endObject());
    }

    @Override
    public Object[] values(Media t) {
        return new Object[] {
                t.getId(),
                t.getType(),
                t.getSubType(),
                t.getFileName(),
                t.getHash(),
                t.getSize(),
                ByteBuffer.wrap(t.getData()),
                t.getApi(),
                t.getCreatedAt()
        };
    }

    // CREATE TABLE workflows(id text primary key, name text, notifier text, config text, hooks list<text>, reference_type text, reference_id text, created_at timestamp, updatedAt timestamp, use_system_proxy boolean)
    @Override
    public Media fromRow(Row row) {
        if (row != null) {
            final Media t = new Media();
            t.setId(row.getString("id"));
            t.setType(row.getString("type"));
            t.setSubType(row.getString("sub_type"));
            t.setFileName(row.getString("file_name"));
            t.setHash(row.getString("hash"));
            t.setSize(row.getLong("size"));
            t.setData(Bytes.getArray(row.getBytes("data")));
            t.setApi(row.getString("api"));
            t.setCreatedAt(row.getTimestamp("created_at"));
            return t;
        }
        return null;
    }

    @Override
    public String save(Media media) throws TechnicalException {
        media.setCreatedAt(new Date());
        return create(media).getId();
    }

    @Override
    public Optional<Media> findByHash(String hash, String type) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("hash", hash))
                .filter(QueryBuilders.termQuery("type", type));
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return Optional.ofNullable(fromRow(resultSet.one()));
    }

    @Override
    public Optional<Media> findByHash(String hash, String api, String mediaType) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(QueryBuilders.termQuery("api", api))
                .filter(QueryBuilders.termQuery("hash", hash))
                .filter(QueryBuilders.termQuery("type", mediaType));
        String esQuery = new SearchSourceBuilder()
                .query(queryBuilder)
                .toString(ToXContent.EMPTY_PARAMS);
        LOGGER.debug("query={}", esQuery);
        final ResultSet resultSet = session.execute(this.esQueryStmt.bind(esQuery));
        return Optional.ofNullable(fromRow(resultSet.one()));
    }


}
