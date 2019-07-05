package io.gravitee.repository.elassandra.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.management.api.EntrypointRepository;
import io.gravitee.repository.management.model.Entrypoint;

@Repository
public class ElassandraEntrypointRepository extends ElassandraCrud<Entrypoint, String> implements EntrypointRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraEntrypointRepository.class);

    public ElassandraEntrypointRepository() {
        super("entrypoints",
                new String[]{"id", "value", "tags" },
                new String[]{"text", "text", "text" },
                1, 1);
    }

    @Override
    public Object[] values(Entrypoint t) {
        return new Object[] {
                t.getId(),
                t.getValue(),
                t.getTags()
        };
    }

    @Override
    public Entrypoint fromRow(Row row) {
        if (row != null) {
            final Entrypoint entrypoint = new Entrypoint();
            entrypoint.setId(row.getString("id"));
            entrypoint.setValue(row.getString("value"));
            entrypoint.setTags(row.getString("tags"));
            return entrypoint;
        }
        return null;
    }
}
