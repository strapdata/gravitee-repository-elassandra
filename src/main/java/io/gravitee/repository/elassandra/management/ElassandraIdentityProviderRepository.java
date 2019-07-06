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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.gravitee.repository.management.api.IdentityProviderRepository;
import io.gravitee.repository.management.model.IdentityProvider;
import io.gravitee.repository.management.model.IdentityProviderType;

/**
 *
 * @author vroyer
 *
 */
@Repository
public class ElassandraIdentityProviderRepository extends ElassandraCrud<IdentityProvider, String> implements IdentityProviderRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraIdentityProviderRepository.class);

    private final static ObjectMapper JSON_MAPPER = new ObjectMapper();

    public ElassandraIdentityProviderRepository() throws IOException {
        super("identity_providers",
                new String[] { "id", "name", "description", "enabled", "provider_type", "configuration","group_mappings","role_mappings","user_profile_mapping","email_required","created_at","updated_at" },
                new String[] { "text", "text", "text", "boolean", "text", "text","text","text","text","boolean","timestamp","timestamp" },
                1,1);
    }

    @Override
    public Object[] values(IdentityProvider a) {
        return new Object[] {
                a.getId(),
                a.getName(),
                a.getDescription(),
                a.isEnabled(),
                a.getType() == null ? null :a.getType().toString(),
                serialize(a.getConfiguration()),
                serialize(a.getGroupMappings()),
                serialize(a.getRoleMappings()),
                serialize(a.getUserProfileMapping()),
                a.getEmailRequired(),
                a.getCreatedAt(),
                a.getUpdatedAt()
        };
    }

    @Override
    public IdentityProvider fromRow(Row row) {
        if (row != null) {
            final IdentityProvider a = new IdentityProvider();
            a.setId(row.getString("id"));
            a.setName(row.getString("name"));
            a.setDescription(row.getString("description"));
            a.setEnabled(row.isNull("enabled") ? null : row.getBool("enabled"));
            a.setType(IdentityProviderType.valueOf(row.getString("provider_type")));
            a.setConfiguration(deserialize(row.getString("configuration"), Object.class, false));
            a.setGroupMappings(deserialize(row.getString("group_mappings"), String.class, true));
            a.setRoleMappings(deserialize(row.getString("role_mappings"), String.class, true));
            a.setUserProfileMapping(deserialize(row.getString("user_profile_mapping"), String.class, false));
            a.setEmailRequired(row.isNull("email_required") ? null : row.getBool("email_required"));
            a.setCreatedAt(row.getTimestamp("created_at"));
            a.setUpdatedAt(row.getTimestamp("updated_at"));
            return a;
        }
        return null;
    }

    private <T, C> Map<String, T> deserialize(String sMap, Class<C> valueType, boolean array) {
        TypeReference<HashMap<String, T>> typeRef
                = new TypeReference<HashMap<String, T>>() {};
        if (sMap != null && ! sMap.isEmpty()) {
            try {
                HashMap<String, Object> value = JSON_MAPPER.readValue(sMap, typeRef);
                if (array) {
                    value
                            .forEach(new BiConsumer<String, Object>() {
                                @Override
                                public void accept(String s, Object t) {
                                    List<C> list = (List<C>) t;
                                    C[] arr = (C[]) Array.newInstance(valueType, list.size());
                                    arr = list.toArray(arr);
                                    value.put(s, arr);
                                }
                            });
                }

                return (Map<String, T>) value;
            } catch (IOException e) {
            }
        }

        return null;
    }

    private String serialize(Map object) {
        if (object != null) {
            try {
                return JSON_MAPPER.writeValueAsString(object);
            } catch (JsonProcessingException e) {
            }
        }

        return null;
    }

}
