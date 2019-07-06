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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.management.api.EntrypointRepository;
import io.gravitee.repository.management.model.Entrypoint;

@Repository
public class ElassandraEntrypointRepository extends ElassandraCrud<Entrypoint, String> implements EntrypointRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraEntrypointRepository.class);

    public ElassandraEntrypointRepository() {
        super("entrypoints",
                new String[]{"id", "value", "tags" },
                new String[]{"text", "text", "text" },
                1, 1);
    }

    @Override
    public Object[] values(Entrypoint t) {
        return new Object[] {
                t.getId(),
                t.getValue(),
                t.getTags()
        };
    }

    @Override
    public Entrypoint fromRow(Row row) {
        if (row != null) {
            final Entrypoint entrypoint = new Entrypoint();
            entrypoint.setId(row.getString("id"));
            entrypoint.setValue(row.getString("value"));
            entrypoint.setTags(row.getString("tags"));
            return entrypoint;
        }
        return null;
    }
}
