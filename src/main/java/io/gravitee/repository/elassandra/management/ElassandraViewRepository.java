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

import io.gravitee.repository.management.api.ViewRepository;
import io.gravitee.repository.management.model.View;

/**
 * @author vroyer
 */
@Repository
public class ElassandraViewRepository extends ElassandraCrud<View, String> implements ViewRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraViewRepository.class);

    public ElassandraViewRepository() {
        super("views",
                new String[] {
                        "id",
                        "name",
                        "description",
                        "default_view",
                        "hidden",
                        "view_order",
                        "created_at",
                        "updated_at",
                        "picture",
                        "highlight_api"
                },
                new String[] {
                        "text",
                        "text",
                        "text",
                        "boolean",
                        "boolean",
                        "int",
                        "timestamp",
                        "timestamp",
                        "text",
                        "text"
                }, 1, 1);
    }

    @Override
    public Object[] values(View t) {
        return new Object[] {
                t.getId(),
                t.getName(),
                t.getDescription(),
                t.isDefaultView(),
                t.isHidden(),
                t.getOrder(),
                t.getCreatedAt(),
                t.getUpdatedAt(),
                t.getPicture(),
                t.getHighlightApi()
        };
    }

    @Override
    public View fromRow(Row row) {
        if (row != null) {
            final View view = new View();
            view.setId(row.getString("id"));
            view.setName(row.getString("name"));
            view.setDescription(row.getString("description"));
            view.setDefaultView(row.isNull("default_view") ? null : row.getBool("default_view"));
            view.setHidden(row.isNull("hidden") ? null : row.getBool("hidden"));
            view.setOrder(row.getInt("view_order"));
            view.setCreatedAt(row.getTimestamp("created_at"));
            view.setUpdatedAt(row.getTimestamp("updated_at"));
            view.setPicture(row.getString("picture"));
            view.setHighlightApi(row.getString("highlight_api"));
            return view;
        }
        return null;
    }
}
