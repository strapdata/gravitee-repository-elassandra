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

import com.datastax.driver.core.Row;
import io.gravitee.repository.management.api.QualityRuleRepository;
import io.gravitee.repository.management.model.QualityRule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.stereotype.Repository;

import java.io.IOException;

@Repository
public class ElassandraQualityRuleRepository extends ElassandraCrud<QualityRule, String> implements QualityRuleRepository {
    public ElassandraQualityRuleRepository() throws IOException {
        super("quality_rules",
                new String[]{"id", "name", "description", "weight", "created_at", "updated_at"},
                new String[]{"text", "text", "text", "int", "timestamp", "timestamp"},
                "quality_rules",
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
                        .startObject("name")
                        .field("type", "keyword")
                        .field("cql_collection", "singleton")
                        .endObject()
                        .endObject()
                        .endObject());
    }

    @Override
    public Object[] values(QualityRule qualityRule) {
        return new Object[] {
                qualityRule.getId(),
                qualityRule.getName(),
                qualityRule.getDescription(),
                qualityRule.getWeight(),
                qualityRule.getCreatedAt(),
                qualityRule.getUpdatedAt()
        };
    }

    @Override
    public QualityRule fromRow(Row row) {
        QualityRule rule = new QualityRule();
        rule.setId(row.getString("id"));
        rule.setName(row.getString("name"));
        rule.setDescription(row.getString("description"));
        rule.setWeight(row.getInt("weight"));
        rule.setCreatedAt(row.getTimestamp("created_at"));
        rule.setUpdatedAt(row.getTimestamp("updated_at"));
        return rule;
    }
}
