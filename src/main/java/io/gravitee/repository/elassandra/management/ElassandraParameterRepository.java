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
