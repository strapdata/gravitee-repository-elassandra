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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.management.api.TagRepository;
import io.gravitee.repository.management.model.Tag;

/**
 * @author vroyer
 */
@Repository
public class ElassandraTagRepository extends ElassandraCrud<Tag, String> implements TagRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraTagRepository.class);

    public ElassandraTagRepository() {
        super("tags",
                new String[] {"id","name","description", "restricted_groups"},
                new String[] {"text","text","text", "list<text>"}, 1, 1);
    }

    @Override
    public Object[] values(Tag t) {
        return new Object[] {
                t.getId(),
                t.getName(),
                t.getDescription(),
                t.getRestrictedGroups()
        };
    }

    @Override
    public Tag fromRow(Row row) {
        if (row != null) {
            final Tag tag = new Tag();
            tag.setId(row.getString("id"));
            tag.setName(row.getString("name"));
            tag.setDescription(row.getString("description"));
            tag.setRestrictedGroups(row.getList("restricted_groups", String.class));
            return tag;
        }
        return null;
    }

}
