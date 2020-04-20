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

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import io.gravitee.repository.elassandra.common.AbstractElassandraRepositoryConfiguration;
import io.gravitee.repository.exceptions.TechnicalException;

/**
 *
 * @author vroyer@strapdata.com
 *
 * @param <T>
 * @param <K>
 */
public abstract class ElassandraCrud<T, K> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraCrud.class);

    @Autowired
    protected Session session;

    @Autowired
    private Environment environment;

    @Autowired
    protected AbstractElassandraRepositoryConfiguration.Ssl ssl;

    @Autowired
    protected AbstractElassandraRepositoryConfiguration.Config config;

    /**
     * Cassandra table name
     */
    public final String tableName;

    /**
     * Elasticsearch index name
     */
    public final String indexName;


    /**
     * Primary key length
     */
    public final int pkLength;

    /**
     * Partition key length
     */
    public final int ptLength;

    /**
     * Cassandra columns starting by primary key columns.
     */
    public final String[] cols;

    /**
     * Cassandra columns CQL types
     */
    public final String[] cqlTypes;

    /**
     * Cassandra clustering key ordering
     */
    public final boolean[] cqlClusteringDesc;

    /**
     * Elasticsearch mapping
     */
    public final XContentBuilder mapping;

    /**
     * Elasticsearch settings
     */
    public final Settings.Builder setting;

    protected PreparedStatement insertStmt;
    protected PreparedStatement deleteStmt;
    protected PreparedStatement deletePartitionStmt;
    protected PreparedStatement selectStmt;
    protected PreparedStatement selectAllStmt;
    protected PreparedStatement esQueryStmt;
    protected PreparedStatement esQueryStmtWithLimit;

    public abstract Object[] values(T t);
    public abstract T fromRow(Row row);

    public Object[] pk(Object[] values) {
        Object[] pkValues = new Object[pkLength];
        System.arraycopy(values, 0, pkValues, 0, pkLength);
        return pkValues;
    }

    /**
     * @param tableName
     * @param cols
     * @param cqlTypes
     * @param ptLength
     * @param clusteringDesc
     */
    public ElassandraCrud(String tableName, String[] cols, String[] cqlTypes, int ptLength, boolean[] clusteringDesc) {
        this.tableName = tableName;
        this.indexName = null;
        this.setting = null;
        this.mapping = null;
        this.cols = cols;
        this.cqlTypes = cqlTypes;
        this.ptLength = ptLength;
        this.pkLength = ptLength + clusteringDesc.length;
        this.cqlClusteringDesc = clusteringDesc;
    }

    public ElassandraCrud(String tableName, String[] cols, String[] cqlTypes, int ptLength, int pkLength) {
        this(tableName, cols, cqlTypes, ptLength, new boolean[pkLength - ptLength]);
    }

    public ElassandraCrud(String tableName, String[] cols, String indexName, Settings.Builder settingBuilder, XContentBuilder mappingBuilder) {
        this(tableName, cols, null, indexName, settingBuilder, mappingBuilder);
    }

    public ElassandraCrud(String tableName, String[] cols, String[] cqlTypes, String indexName, Settings.Builder settingBuilder, XContentBuilder mappingBuilder) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.setting = settingBuilder;
        this.mapping = mappingBuilder;
        this.cols = cols;
        this.cqlTypes = cqlTypes;

        Map<String, Object> properties = (Map<String, Object>) XContentHelper.convertToMap(BytesReference.bytes(this.mapping), true, null).v2().get("properties");
        int ptLen = 0;
        int pkLen = 0;
        for(String key : properties.keySet()) {
            Map<String, Object> fieldMapping = (Map<String, Object>)properties.get(key);
            if (fieldMapping.containsKey("cql_primary_key_order"))
                pkLen++;
            if (Boolean.TRUE.equals(fieldMapping.get("cql_partition_key")))
                ptLen++;
        }
        this.ptLength = ptLen;
        this.pkLength = pkLen;
        assert pkLength >= 1 : "primary key length is zero, table="+tableName+" mapping="+properties;
        assert ptLength >= 1 : "partition key length is zero, table="+tableName+" mapping="+properties;
        this.cqlClusteringDesc = new boolean[pkLength - ptLength];
        for(int i = 0; i < cqlClusteringDesc.length; i++) {
            String clusteringColName = cols[i];
            Map<String, Object> fieldMapping = (Map<String, Object>)properties.get(clusteringColName);
            cqlClusteringDesc[i] = Boolean.TRUE.equals(fieldMapping.get("cql_clustering_key_desc"));
        }
    }

    private String buildWhereClause() {
        StringBuffer sb = new StringBuffer(cols[0] + "= ?");
        for(int i= 1; i < pkLength; i++)
            sb.append(" AND " + cols[i] + " = ?");
        return sb.toString();
    }

    private String buildPartitionWhereClause() {
        StringBuffer sb = new StringBuffer(cols[0] + "= ?");
        for(int i= 1; i < ptLength; i++)
            sb.append(" AND " + cols[i] + " = ?");
        return sb.toString();
    }

    protected String buildProjectionClause() {
        StringBuffer sb = new StringBuffer(cols[0]);
        for(int i= 1; i < cols.length; i++)
            sb.append(", " + cols[i]);
        return sb.toString();
    }

    protected String buildMarksClause() {
        StringBuffer sb = new StringBuffer("?");
        for(int i= 1; i < cols.length; i++)
            sb.append(",?");
        return sb.toString();
    }

    protected String buildCreateClause() {
        StringBuffer sb = new StringBuffer(cols[0] + " "+cqlTypes[0]);
        for(int i= 1; i < cols.length; i++)
            sb.append(", " + cols[i] + " " + cqlTypes[i]);
        return sb.toString();
    }

    protected String buildPkClause() {
        StringBuffer sb = new StringBuffer("(" + cols[0]);
        for(int i= 1; i < ptLength; i++)
            sb.append(", " + cols[i]);
        sb.append(")");
        for(int i = ptLength; i < pkLength; i++)
            sb.append(", " + cols[i]);
        return sb.toString();
    }

    protected String buildCluseringOrderClause() {
        if (cqlClusteringDesc.length == 0)
            return "";
        StringBuffer sb = new StringBuffer(" WITH CLUSTERING ORDER BY ( ");
        for(int i = 0; i < this.cqlClusteringDesc.length; i++) {
            if (i > 0) sb.append(" ");
            sb.append(cols[ptLength + i]).append( cqlClusteringDesc[i] ? " DESC " : " ASC ");
        }
        return sb.append(")").toString();
    }

    @PostConstruct
    public void init() {
        if (session == null){
            LOGGER.error("Cassandra session not available");
            return;
        }

        if (cqlTypes != null)
            initSchema();

        if (indexName != null) {
            try {
                initMapping();
            } catch (KeyStoreException | NoSuchAlgorithmException | IOException e) {
                LOGGER.error("Failed to init elasticsearch mapping", e);
            }
        }

        // init prepared statement
        LOGGER.debug("table={} where={}", tableName, buildWhereClause());

        selectStmt = session.prepare(String.format(Locale.ROOT,"SELECT %s FROM %s WHERE %s", buildProjectionClause(), tableName, buildWhereClause()));
        selectAllStmt = session.prepare(String.format(Locale.ROOT,"SELECT %s FROM %s", buildProjectionClause(), tableName));
        deleteStmt = session.prepare(String.format(Locale.ROOT,"DELETE FROM %s WHERE %s", tableName, buildWhereClause()));
        if (ptLength != pkLength) {
            deletePartitionStmt = session.prepare(String.format(Locale.ROOT,"DELETE FROM %s WHERE %s", tableName, buildPartitionWhereClause()));
        } else {
            deletePartitionStmt = deleteStmt;
        }
        insertStmt = session.prepare(String.format(Locale.ROOT,"INSERT INTO %s (%s) VALUES (%s)", tableName, buildProjectionClause(), buildMarksClause()));
        if (indexName != null) {
            try {
                session.execute(String.format(Locale.ROOT,"ALTER TABLE %s ADD es_query text;", tableName));
                session.execute(String.format(Locale.ROOT,"ALTER TABLE %s ADD es_options text;", tableName));
            } catch(Exception e) {
                //LOGGER.warn("alter table", e);
            }
            esQueryStmt = session.prepare(String.format(Locale.ROOT,"SELECT %s FROM %s WHERE es_query = ? AND es_options='indices=%s' ALLOW FILTERING", buildProjectionClause(), tableName, indexName));
            esQueryStmtWithLimit = session.prepare(String.format(Locale.ROOT,"SELECT %s FROM %s WHERE es_query = ? AND es_options='indices=%s' LIMIT ? ALLOW FILTERING", buildProjectionClause(), tableName, indexName));
        }
    }

    public void initMapping() throws IOException, KeyStoreException, NoSuchAlgorithmException {
        LOGGER.info("Init Elasticsearch endpoint={} username={} password=*********", config.getEndpoint(), config.getUsername());

        try(RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(HttpHost.create(config.getEndpoint()))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                        HttpAsyncClientBuilder httpClientBuilder) {
                        if (config.getUsername() != null && config.getPassword() != null) {
                            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }

                        if (ssl.getSslContext() != null) {
                            LOGGER.debug("SSL context added");
                            httpClientBuilder.setSSLContext(ssl.getSslContext());
                            // TODO: fix this workaround
                            httpClientBuilder.setSSLHostnameVerifier(new HostnameVerifier() {
                                @Override
                                public boolean verify(String arg0, SSLSession arg1) {
                                    // TODO Auto-generated method stub
                                    return true;
                                }
                            });
                        }
                        return httpClientBuilder;
                    }
                }))
        ) {
            for(String index : new String [] { indexName } ) {
                try {
                    String effectiveIndexName = config.getIndexPrefix()+index;
                    CreateIndexRequest request = new CreateIndexRequest(effectiveIndexName);
                    request.mapping(tableName, mapping);
                    request.settings(setting.put("index.keyspace", session.getLoggedKeyspace()));
                    CreateIndexResponse createIndexResponse = client.indices().create(request);
                    LOGGER.info("Elasticsearch index {} created", index);
                } catch(org.elasticsearch.ElasticsearchStatusException e) {
                    if (!(e.getCause() instanceof org.elasticsearch.ResourceAlreadyExistsException))
                    LOGGER.warn("error status=" + e.status(), e.getCause());
                } catch(ElasticsearchException e) {
                    LOGGER.warn("Failed to create index="+index, e);
                }
            }
        } catch(Exception e) {
            LOGGER.error("Failed to create elasticsearch index", e);
        }
    }

    public void initSchema() {
        String createTable = String.format(Locale.ROOT,"CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY ( %s )) %s", tableName,
                buildCreateClause(), buildPkClause(), buildCluseringOrderClause());
        LOGGER.debug(createTable);
        session.execute(createTable);
    }

    public T create(T t) throws TechnicalException {
        if (t == null)
            throw new IllegalStateException("cannot upsert null object");
        Object[] pkCols = pk(values(t));
        for(int i=0; i < pkCols.length; i++)
            if (pkCols[i] == null)
                throw new IllegalStateException("Primary key column["+i+"] is null");
        try {
            LOGGER.debug("t={}",t);
            session.execute(insertStmt.bind(values(t)));
            return fromRow(session.execute(selectStmt.bind(pkCols)).one());
        } catch (final Exception ex) {
            String message = String.format("Failed to upsert table=%s primary key=%s", tableName, Arrays.toString(pkCols));
            LOGGER.error(message, ex);
            throw new TechnicalException(message, ex);
        }
    }

    public void create(List<T> tList) throws TechnicalException {
        for(T t : tList)
            create(t);
    }

    public T update(T t) throws TechnicalException {
        if (t == null)
           throw new IllegalStateException("cannot update null object");
        Object[] pkCols = pk(values(t));
        for(int i=0; i < pkCols.length; i++)
            if (pkCols[i] == null)
                throw new IllegalStateException("Primary key column["+i+"] is null");
        if (session.execute(selectStmt.bind(pkCols)).one() == null)
           throw new IllegalStateException(String.format("No object found in [%s] with primary key %s", tableName, Arrays.toString(pkCols)));
        return create(t);
    }

    public Optional<T> findById(K k) throws TechnicalException {
        if (k == null)
            throw new IllegalStateException("No primary key column");
        T t  = fromRow(session.execute(selectStmt.bind(k)).one());
        LOGGER.debug("t={}", t);
        return Optional.ofNullable(t);
    }

    public void delete(K k) throws TechnicalException {
        if (k == null)
            throw new IllegalStateException("cannot delete null primary key");
        session.execute(deleteStmt.bind(k));
    }

    public void deletePartition(Object ... arg) throws TechnicalException {
        if (arg == null)
            throw new IllegalStateException("cannot delete null partition key");
        session.execute(deletePartitionStmt.bind(arg));
    }

    public Set<T> findAll() throws TechnicalException {
        return session.execute(selectAllStmt.bind()).all().stream().map(this::fromRow).collect(Collectors.toSet());
    }

    public List<T> findAll(List<K> kList) throws TechnicalException {
        List<T> result = new ArrayList<>();
        for(K k : kList) {
            Optional<T> o = findById(k);
            if (o.isPresent())
                result.add(o.get());
        }
        return result;
    }
}
