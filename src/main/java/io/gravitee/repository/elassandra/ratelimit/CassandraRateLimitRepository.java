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
package io.gravitee.repository.elassandra.ratelimit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;

import java.util.Date;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.gravitee.repository.elassandra.management.ElassandraCrud;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.ratelimit.api.RateLimitRepository;
import io.gravitee.repository.ratelimit.model.RateLimit;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 * @author vroyer
 */
@Repository
public class CassandraRateLimitRepository extends ElassandraCrud<RateLimit, String> implements RateLimitRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRateLimitRepository.class);

    public CassandraRateLimitRepository() {
        super("ratelimits",
                new String[]{"id", "last_request", "counter", "reset_time", "created_at", "updated_at", "async"},
                new String[] {"text","timestamp","bigint", "timestamp", "timestamp", "timestamp", "boolean"},
                1, 1);
    }

    @Override
    public Object[] values(RateLimit rateLimit) {
        return new Object[]{
                rateLimit.getKey(),
                rateLimit.getLastRequest(),
                rateLimit.getCounter(),
                new Date(rateLimit.getResetTime()),
                new Date(rateLimit.getCreatedAt()),
                new Date(rateLimit.getUpdatedAt()),
                rateLimit.isAsync()};
    }

    @Override
    public RateLimit fromRow(Row row) {
        if (row != null) {
            final RateLimit rateLimit = new RateLimit(row.getString("id"));
            rateLimit.setLastRequest(row.getLong("last_request"));
            rateLimit.setCounter(row.getLong("counter"));
            rateLimit.setResetTime(row.getTimestamp("reset_time").getTime());
            rateLimit.setCreatedAt(row.getTimestamp("created_at").getTime());
            rateLimit.setUpdatedAt(row.getTimestamp("updated_at").getTime());
            rateLimit.setAsync(row.getBool("async"));
            return rateLimit;
        }
        return null;
    }

    @Override
    public RateLimit get(String rateId) {
        final Statement select = QueryBuilder.select().all().from(tableName).where(eq("id", rateId));
        final Row row = session.execute(select).one();
        RateLimit rateLimit = fromRow(row);

        if (rateLimit == null) {
            rateLimit = new RateLimit(rateId);
        }
        return rateLimit;
    }

    @Override
    public void save(RateLimit rateLimit) {
        try {
            create(rateLimit);
        } catch (TechnicalException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<RateLimit> findAsyncAfter(long timestamp) {
        final Statement select = QueryBuilder.select().all().from(tableName).allowFiltering()
                .where(eq("async", true))
                .and(gte("updated_at", timestamp));

        final Iterator<Row> rows = session.execute(select).iterator();

        return new Iterator<RateLimit>() {

            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public RateLimit next() {
                return fromRow(rows.next());
            }
        };
    }

}
