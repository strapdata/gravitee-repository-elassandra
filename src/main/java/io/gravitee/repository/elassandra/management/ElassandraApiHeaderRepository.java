/**
 * Copyright (C) ${project.inceptionYear} Strapdata (https://www.strapdata.com)
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
