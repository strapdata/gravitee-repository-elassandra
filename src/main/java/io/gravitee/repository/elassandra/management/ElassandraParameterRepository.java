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

import io.gravitee.repository.management.api.ParameterRepository;
import io.gravitee.repository.management.model.Parameter;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraParameterRepository extends ElassandraCrud<Parameter, String> implements ParameterRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraDictionaryRepository.class);

    public ElassandraParameterRepository() {
        super("parameters",
              new String[] {"key","value"},
              new String[] {"text","text"}, 1, 1);
    }

    @Override
    public Object[] values(Parameter t) {
        return new Object[] {
                t.getKey(),
                t.getValue()
        };
    }

    @Override
    public Parameter fromRow(Row row) {
        if (row != null) {
            final Parameter parameter = new Parameter();
            parameter.setKey(row.getString("key"));
            parameter.setValue(row.getString("value"));
            return parameter;
        }
        return null;
    }
}
