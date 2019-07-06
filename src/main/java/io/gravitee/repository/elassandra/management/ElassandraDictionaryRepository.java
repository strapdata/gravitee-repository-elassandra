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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.management.api.DictionaryRepository;
import io.gravitee.repository.management.model.Dictionary;
import io.gravitee.repository.management.model.DictionaryProvider;
import io.gravitee.repository.management.model.DictionaryTrigger;
import io.gravitee.repository.management.model.DictionaryType;
import io.gravitee.repository.management.model.LifecycleState;

@Repository
public class ElassandraDictionaryRepository extends ElassandraCrud<Dictionary, String> implements DictionaryRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraDictionaryRepository.class);

    public ElassandraDictionaryRepository() {
        super("dictionaries",
                new String[]{"id", "name", "description", "type", "created_at", "updated_at", "state", "properties", "provider_type", "provider_configuration", "trigger_rate", "trigger_time_unit" },
                new String[]{"text", "text", "text", "text", "timestamp", "timestamp", "text", "map<text,text>", "text", "text", "bigint", "text" },
                1, 1);
    }

    @Override
    public Object[] values(Dictionary t) {
        return new Object[]{
                t.getId(),
                t.getName(),
                t.getDescription(),
                t.getType() == null ? null : t.getType().toString(),
                t.getCreatedAt(),
                t.getUpdatedAt(),
                t.getState() == null ? null : t.getState().name(),
                t.getProperties(),
                t.getProvider() == null ? null : t.getProvider().getType(),
                t.getProvider() == null ? null : t.getProvider().getConfiguration(),
                t.getTrigger() != null ? t.getTrigger().getRate() : null,
                t.getTrigger() != null && t.getTrigger().getUnit() != null ? t.getTrigger().getUnit().toString() : null};
    }

    @Override
    public Dictionary fromRow(Row row) {
        if (row != null) {
            final Dictionary d = new Dictionary();
            d.setId(row.getString("id"));
            d.setName(row.getString("name"));
            d.setDescription(row.getString("description"));
            d.setType(row.getString("type") == null ? null : DictionaryType.valueOf(row.getString("type")));
            d.setCreatedAt(row.getTimestamp("created_at"));
            d.setUpdatedAt(row.getTimestamp("updated_at"));
            d.setState(row.getString("state") == null ? null : LifecycleState.valueOf(row.getString("state")));
            d.setProperties(row.getMap("properties", String.class, String.class));

            DictionaryProvider dp = new DictionaryProvider();
            dp.setType(row.getString("provider_type"));
            dp.setConfiguration(row.getString("provider_configuration"));
            d.setProvider(dp);

            DictionaryTrigger dt = new DictionaryTrigger();
            dt.setRate(row.getLong("trigger_rate"));
            dt.setUnit(row.getString("trigger_time_unit") == null ? null : TimeUnit.valueOf(row.getString("trigger_time_unit")));
            d.setTrigger(dt);
            return d;
        }
        return null;
    }

}
