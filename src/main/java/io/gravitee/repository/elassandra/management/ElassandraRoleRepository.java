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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.RoleRepository;
import io.gravitee.repository.management.model.Role;
import io.gravitee.repository.management.model.RoleScope;

/**
 * @author vroyer
 */
@Repository
public class ElassandraRoleRepository extends ElassandraCrud<Role, Object[]> implements RoleRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraRoleRepository.class);

    public ElassandraRoleRepository() {
        super("roles",
                new String[] {"scope", "name", "description", "is_default", "permissions", "system","created_at", "updated_at"},
                new String[] {"int", "text", "text", "boolean", "list<int>", "boolean","timestamp", "timestamp"},
                1, 2);
    }


    @Override
    public Object[] values(Role role) {
        List<Integer> perms = new ArrayList<>(role.getPermissions() == null ? 0 : role.getPermissions().length);
        if (role.getPermissions() != null) {
            for (int perm : role.getPermissions())
                perms.add(perm);
        }
        return new Object[]{
                role.getScope() == null ? null : role.getScope().getId(),
                role.getName(),
                role.getDescription(),
                role.isDefaultRole(),
                perms,
                role.isSystem(),
                role.getCreatedAt(),
                role.getUpdatedAt() };
    }

    @Override
    public Role fromRow(Row row) {
        if (row != null) {
            final Role role = new Role();
            role.setName(row.getString("name"));
            role.setScope(row.isNull("scope") ? null : RoleScope.valueOf(row.getInt("scope")));
            role.setDescription(row.getString("description"));
            role.setDefaultRole(row.isNull("is_default") ? null : row.getBool("is_default"));
            role.setSystem(row.isNull("system") ? null : row.getBool("system"));
            List<Integer> permissions = row.getList("permissions", Integer.class);
            if (permissions != null) {
                int[] ints = new int[permissions.size()];
                for (int i = permissions.size() - 1; i >= 0; i--) {
                    ints[i] = permissions.get(i);
                }
                role.setPermissions(ints);
            }
            role.setCreatedAt(row.getTimestamp("created_at"));
            role.setUpdatedAt(row.getTimestamp("updated_at"));
            return role;
        }
        return null;
    }

    @Override
    public Optional<Role> findById(RoleScope scope, String name) throws TechnicalException {
        if (scope == null || name == null)
            return Optional.empty();
        LOGGER.debug("findById Role [{}, {}]", scope, name);
        return Optional.ofNullable(fromRow(session.execute(selectStmt.bind(scope.getId(), name)).one()));
    }

    @Override
    public void delete(RoleScope scope, String name) throws TechnicalException {
        if (scope == null || name == null)
            throw new IllegalStateException("No primary key column");
        LOGGER.debug("Delete Role [{}, {}]", scope, name);
        session.execute(deleteStmt.bind(scope.getId(), name));
    }

    @Override
    public Set<Role> findByScope(RoleScope scope) throws TechnicalException {
        LOGGER.debug("Find Role by Scope [{}]", scope);

        final Statement select = QueryBuilder.
                select().
                all().
                from(this.tableName).
                where(eq("scope", scope.getId()));

        return session.execute(select).
                all().
                stream().
                map(this::fromRow).
                collect(Collectors.toSet());
    }
}
