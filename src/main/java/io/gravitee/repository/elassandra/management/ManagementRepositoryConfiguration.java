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

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import io.gravitee.repository.Scope;
import io.gravitee.repository.elassandra.common.AbstractElassandraRepositoryConfiguration;

/**
 * @author vroyer
 */
@Configuration
@ComponentScan(basePackages = {"io.gravitee.repository.elassandra.management"})
public class ManagementRepositoryConfiguration extends AbstractElassandraRepositoryConfiguration {

    @Override
    protected Scope getScope() {
        return Scope.MANAGEMENT;
    }

}
