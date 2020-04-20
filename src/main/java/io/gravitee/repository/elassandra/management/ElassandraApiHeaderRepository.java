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

import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.management.api.ApiHeaderRepository;
import io.gravitee.repository.management.model.ApiHeader;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraApiHeaderRepository extends ElassandraCrud<ApiHeader, String> implements ApiHeaderRepository {

    public ElassandraApiHeaderRepository() throws IOException {
        super("api_headers",
                new String[] { "id", "name", "value", "api_order", "created_at", "updated_at" },
                new String[] { "text", "text", "text", "int", "timestamp", "timestamp" },
                1,1);
    }

    @Override
    public Object[] values(ApiHeader a) {
        return new Object[] {
                a.getId(),
                a.getName(),
                a.getValue(),
                a.getOrder(),
                a.getCreatedAt(),
                a.getUpdatedAt()
        };
    }

    @Override
    public ApiHeader fromRow(Row row) {
        if (row != null) {
            final ApiHeader a = new ApiHeader();
            a.setId(row.getString("id"));
            a.setName(row.getString("name"));
            a.setValue(row.getString("value"));
            a.setOrder(row.getInt("api_order"));
            a.setCreatedAt(row.getTimestamp("created_at"));
            a.setUpdatedAt(row.getTimestamp("updated_at"));
            return a;
        }
        return null;
    }
}
