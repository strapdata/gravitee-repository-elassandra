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


import static com.datastax.driver.core.querybuilder.QueryBuilder.in;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.GroupRepository;
import io.gravitee.repository.management.model.Group;
import io.gravitee.repository.management.model.GroupEvent;
import io.gravitee.repository.management.model.GroupEventRule;

/**
 * @author vroyer@strapdata.com
 */
@Repository
public class ElassandraGroupRepository extends ElassandraCrud<Group, String> implements GroupRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraGroupRepository.class);

    public ElassandraGroupRepository() {
        super("groups",
                new String[]{ "id", "name", "group_roles", "event_rules", "created_at", "updated_at", "max_invitation", "lock_api_role", "lock_application_role", "system_invitation", "email_invitation"},
                new String[]{ "text", "text", "map<int,text>", "list<text>", "timestamp", "timestamp", "int", "boolean", "boolean", "boolean", "boolean"},
                1, 1);
    }

    @Override
    public Object[] values(Group  group) {
        List<?> eventRules = group.getEventRules() == null
                ? Collections.emptyList()
                : group.getEventRules().
                stream().
                map(groupEventRule -> groupEventRule.getEvent().name()).
                collect(Collectors.toList());

        return new Object[]{
                group.getId(),
                group.getName(),
                group.getRoles(),
                eventRules,
                group.getCreatedAt(),
                group.getUpdatedAt(),
                group.getMaxInvitation(),
                group.isLockApiRole(),
                group.isLockApplicationRole(),
                group.isSystemInvitation(),
                group.isEmailInvitation()};
    }

    @Override
    public Group fromRow(Row row) {
        if (row != null) {
            final Group group = new Group();
            group.setId(row.getString("id"));
            group.setName(row.getString("name"));
            group.setMaxInvitation(row.getInt("max_invitation"));
            group.setLockApiRole(row.isNull("lock_api_role") ? null : row.getBool("lock_api_role"));
            group.setLockApplicationRole(row.isNull("lock_application_role") ? null : row.getBool("lock_application_role"));
            group.setSystemInvitation(row.isNull("system_invitation") ? null : row.getBool("system_invitation"));
            group.setEmailInvitation(row.isNull("email_invitation") ? null : row.getBool("email_invitation"));
            List<String> eventRules = row.getList("event_rules", String.class);
            if (eventRules != null && !eventRules.isEmpty()) {
                group.setEventRules(eventRules.stream().
                        map(event -> new GroupEventRule(GroupEvent.valueOf(event))).
                        collect(Collectors.toList()));
            }
            Map<Integer, String> groupRoles = row.getMap("group_roles", Integer.class, String.class);
            if (groupRoles != null && !groupRoles.isEmpty()) {
                group.setRoles(groupRoles);
            }
            group.setCreatedAt(row.getTimestamp("created_at"));
            group.setUpdatedAt(row.getTimestamp("updated_at"));
            return group;
        }
        return null;
    }

    @Override
    public Set<Group> findByIds(Set<String> ids) throws TechnicalException {
        LOGGER.debug("Find Group by IDs [{}]", ids);

        final Statement select = QueryBuilder.select().all().from(tableName).where(in("id", new ArrayList<>(ids)));
        final ResultSet resultSet = session.execute(select);
        List<Row> all = resultSet.all();
        return all.stream().map(this::fromRow).collect(Collectors.toSet());
    }

}
