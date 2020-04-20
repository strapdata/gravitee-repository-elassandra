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

import java.util.Arrays;
import java.util.List;
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
import io.gravitee.repository.management.api.ApplicationRepository;
import io.gravitee.repository.management.model.Application;
import io.gravitee.repository.management.model.ApplicationStatus;
import io.gravitee.repository.management.model.ApplicationType;

/**
 * @author vroyer@strapdata.com
 * CREATE TABLE IF NOT EXISTS gravitee.applications (id text PRIMARY KEY, name text, description text, type text, created_at timestamp, updated_at timestamp, groups set<text>, status text);

 */
@Repository
public class ElassandraApplicationRepository extends ElassandraCrud<Application, String> implements ApplicationRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraApplicationRepository.class);

    public ElassandraApplicationRepository() {
        super("applications",
                new String[]{"id", "name", "description", "type", "created_at", "updated_at", "groups", "status","metadata"},
                new String[]{"text", "text", "text", "text", "timestamp", "timestamp", "set<text>", "text","map<text,text>"},
                1,1);
    }

    @Override
    public Object[] values(Application application) {
        return new Object[] {
                application.getId(),
                application.getName(),
                application.getDescription(),
                application.getType() == null ? null : application.getType().toString(),
                application.getCreatedAt(),
                application.getUpdatedAt(),
                application.getGroups(),
                application.getStatus() == null ? null : application.getStatus().toString(),
                application.getMetadata()
        };
    }

    @Override
    public Application fromRow(Row row) {
        if (row != null) {
            final Application application = new Application();
            application.setId(row.getString("id"));
            application.setName(row.getString("name"));
            application.setDescription(row.getString("description"));
            application.setType(ApplicationType.valueOf(row.getString("type")));
            application.setCreatedAt(row.getTimestamp("created_at"));
            application.setUpdatedAt(row.getTimestamp("updated_at"));
            application.setGroups(row.getSet("groups", String.class));
            application.setStatus(ApplicationStatus.valueOf(row.getString("status")));
            application.setMetadata(row.getMap("metadata", String.class, String.class));
            return application;
        }
        return null;
    }

    @Override
    public Set<Application> findAll(ApplicationStatus... statuses) throws TechnicalException {
        LOGGER.debug("Find all Applications");

        final Statement select = QueryBuilder.select().all().from(tableName);
        final ResultSet resultSet = session.execute(select);

        Set<Application> applications = resultSet.all().stream().
                map(this::fromRow).
                collect(Collectors.toSet());
        if (statuses != null && statuses.length > 0) {
            List<ApplicationStatus> applicationStatuses = Arrays.asList(statuses);
            applications = applications.stream().
                    filter(app -> applicationStatuses.contains(app.getStatus())).
                    collect(Collectors.toSet());
        }

        LOGGER.debug("Found {} applications", applications.size());
        return applications;
    }

    @Override
    public Set<Application> findByIds(List<String> ids) throws TechnicalException {
        LOGGER.debug("Find Applications by ID list");

        // may be wrong : should loop through list and add resultsets to a list of resultset
        final Statement select = QueryBuilder.select().all().from(tableName).where(in("id", ids));
        final ResultSet resultSet = session.execute(select);

        final Set<Application> applications = resultSet.all().stream().map(this::fromRow).collect(Collectors.toSet());

        LOGGER.debug("Found {} applications", applications.size());
        return applications;
    }

    @Override
    public Set<Application> findByGroups(List<String> groups, ApplicationStatus ... statuses) throws TechnicalException {
        LOGGER.debug("Find Applications by Group list");

        // may be wrong : should loop through list and add resultsets to a list of resultset
        final Statement select = QueryBuilder.select().all().from(tableName);

        final ResultSet resultSet = session.execute(select);

        Set<Application> applications = resultSet.all().stream().
                map(this::fromRow).
                filter(application -> application.getGroups().stream().map(group-> groups.contains(group)).reduce(Boolean::logicalOr).orElse(false)).
                collect(Collectors.toSet());

        if (statuses != null && statuses.length > 0) {
            List<ApplicationStatus> applicationStatuses = Arrays.asList(statuses);
            applications = applications.stream().
                    filter(app -> applicationStatuses.contains(app.getStatus())).
                    collect(Collectors.toSet());
        }

        return applications;
    }

    @Override
    public Set<Application> findByName(String partialName) throws TechnicalException {
        LOGGER.debug("Find Application by partial name [{}]", partialName);

        final Statement select = QueryBuilder.select("id", "name").from(tableName);

        final ResultSet resultSet = session.execute(select);

        final List<String> applicationIds = resultSet.all().stream()
                .filter(row -> row.getString("name").toLowerCase().contains(partialName.toLowerCase()))
                .map(row -> row.getString("id"))
                .collect(Collectors.toList());

        return findByIds(applicationIds);
    }
}
