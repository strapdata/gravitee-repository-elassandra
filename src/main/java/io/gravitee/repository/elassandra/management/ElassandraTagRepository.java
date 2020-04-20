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
