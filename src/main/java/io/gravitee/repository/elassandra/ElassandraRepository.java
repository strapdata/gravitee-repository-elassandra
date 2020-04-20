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
package io.gravitee.repository.elassandra;

import io.gravitee.repository.Repository;
import io.gravitee.repository.Scope;
import io.gravitee.repository.elassandra.management.ManagementRepositoryConfiguration;
import io.gravitee.repository.elassandra.ratelimit.RateLimitRepositoryConfiguration;
import io.gravitee.repository.elasticsearch.spring.ElasticsearchRepositoryConfiguration;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 * @author vroyer
 */
public class ElassandraRepository implements Repository {

    /**
     * @return the type of repository
     */
    @Override
    public String type() {
        return "elassandra";
    }

    /**
     * @return Scopes handled by this repository
     */
    @Override
    public Scope[] scopes() {
        return new Scope[]{ Scope.MANAGEMENT, Scope.RATE_LIMIT, Scope.ANALYTICS };
    }

    /**
     * Get the correct configuration class for specified scope
     * @param scope current scope
     * @return configuration class for current scope
     */
    @Override
    public Class<?> configuration(Scope scope) {
        switch (scope) {
        case ANALYTICS:
            return ElasticsearchRepositoryConfiguration.class;
        case MANAGEMENT:
            return ManagementRepositoryConfiguration.class;
        case RATE_LIMIT:
                return RateLimitRepositoryConfiguration.class;
        }
        return null;
    }
}
