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
package io.gravitee.repository.elassandra.ratelimit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;

import io.reactivex.Single;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.gravitee.repository.elassandra.management.ElassandraCrud;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.ratelimit.api.RateLimitRepository;
import io.gravitee.repository.ratelimit.model.RateLimit;

/**
 * See usage https://github.com/gravitee-io/gravitee-policy-ratelimit/blob/master/gravitee-policy-ratelimit/src/main/java/io/gravitee/policy/ratelimit/RateLimitPolicy.java
 * See https://niels.nu/blog/2016/cassandra-rate-limiting-api.html
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 * @author vroyer@strapdata.com
 */
@Repository
public class ElassandraRateLimitRepository extends ElassandraCrud<RateLimit, String> implements RateLimitRepository<RateLimit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraRateLimitRepository.class);

    protected PreparedStatement insertStmtUsingTtl;

    public ElassandraRateLimitRepository() throws IOException {
        super("ratelimits",
                new String[] { "id", "counter", "reset_time", "rate_limit", "subscription" },
                new String[] { "text", "bigint", "timestamp", "bigint", "text" },
                1,
                new boolean[] { true });
    }

    @Override
    public Single<RateLimit> incrementAndGet(String key, long weight, Supplier<RateLimit> supplier) {
        return Single.fromFuture(session.executeAsync(selectStmt.bind(key)))
                .map((resultSet) -> {
                    RateLimit rateLimit = null;
                    Row row = resultSet.one();
                    if (row == null) {
                        rateLimit = create(supplier.get());
                    } else {
                        rateLimit = fromRow(row);
                        rateLimit.setCounter(rateLimit.getCounter() + weight);
                        rateLimit = create(rateLimit);
                    }
                    return rateLimit;
                });
    }

    @PostConstruct
    public void initStmt() {
        insertStmtUsingTtl = session.prepare(String.format(Locale.ROOT,"INSERT INTO %s (%s) VALUES (%s) USING TTL ?", tableName, buildProjectionClause(), buildMarksClause()));
    }

    @Override
    public Object[] values(RateLimit rateLimit) {
        return new Object[]{
                rateLimit.getKey(),
                rateLimit.getCounter(),
                new Date(rateLimit.getResetTime()),
                rateLimit.getLimit(),
                rateLimit.getSubscription()};
    }

    @Override
    public RateLimit fromRow(Row row) {
        if (row != null) {
            final RateLimit rateLimit = new RateLimit(row.getString("id"));
            rateLimit.setCounter(row.getLong("counter"));
            rateLimit.setResetTime(row.getTimestamp("reset_time").getTime());
            rateLimit.setLimit(row.getLong("rate_limit"));
            rateLimit.setSubscription(row.getString("subscription"));
            return rateLimit;
        }
        return null;
    }

    /**
     * Insert RateLimit using TTL = resetTime - currentTime + 10s
     */
    @Override
    public RateLimit create(RateLimit t) throws TechnicalException {
        if (t == null)
            throw new IllegalStateException("cannot upsert null object");
        Object[] pkCols = pk(values(t));
        for(int i=0; i < pkCols.length; i++)
            if (pkCols[i] == null)
                throw new IllegalStateException("Primary key column["+i+"] is null");
        try {
            long ttl = ((t.getResetTime() - System.currentTimeMillis()) / 1000) + 10;
            assert ttl > 0 : "ratelimits ttl is negative";
            LOGGER.debug("t={} with ttl={}",t, ttl);
            Object[] values = values(t);
            Object[] valuesUsingTtl = new Object[values.length+1];
            System.arraycopy(values,  0,  valuesUsingTtl,  0, values.length);
            valuesUsingTtl[values.length] = (int)ttl;
            session.execute(insertStmtUsingTtl.bind(valuesUsingTtl));
            return fromRow(session.execute(selectStmt.bind(pkCols)).one());
        } catch (final Exception ex) {
            String message = String.format("Failed to upsert table=%s primary key=%s", tableName, Arrays.toString(pkCols));
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }

}
