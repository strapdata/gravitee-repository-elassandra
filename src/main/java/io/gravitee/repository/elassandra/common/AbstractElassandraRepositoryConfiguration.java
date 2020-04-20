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
package io.gravitee.repository.elassandra.common;

import static java.nio.file.Paths.get;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Enumeration;
import java.util.Locale;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import io.gravitee.repository.Scope;
import io.gravitee.repository.elassandra.management.transaction.NoTransactionManager;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/**
 * Common configuration for creating Cassandra Driver cluster and session with the provided options.
 *
 * @author vroyer
 */
public abstract class AbstractElassandraRepositoryConfiguration {
    private final Logger LOGGER = LoggerFactory.getLogger(AbstractElassandraRepositoryConfiguration.class);

    @Autowired
    private Environment environment;

    private String scope;

    private SSLContext sslContext = null;

    protected abstract Scope getScope();

    public AbstractElassandraRepositoryConfiguration() {
        this.scope = getScope().getName();
    }

    /**
     * @return Cassandra Cluster object
     */
    @Bean(destroyMethod = "close")
    public Cluster cluster() {
        LOGGER.debug("Building Cassandra Cluster object, scope={}", scope);

        DCAwareRoundRobinPolicy.Builder dcAwareBuilder = new DCAwareRoundRobinPolicy.Builder();
        if (environment.getProperty(scope + ".elassandra.localDc") != null)
            dcAwareBuilder.withLocalDc(environment.getProperty(scope + ".elassandra.localDc"));
        LoadBalancingPolicy lbp = new TokenAwarePolicy(dcAwareBuilder.build());

        // TODO: add configurable LatencyAwarePolicy, see https://docs.datastax.com/en/developer/java-driver/3.6/manual/load_balancing/
        Cluster.Builder builder= Cluster.builder()
                .addContactPoints(environment.getProperty(scope + ".elassandra.contactPoint", "localhost"))
                .withPort(environment.getProperty(scope + ".elassandra.port", Integer.class, 9042))
                .withClusterName(environment.getProperty(scope + ".elassandra.clusterName", "elassandra"))
                .withCredentials(
                        environment.getProperty(scope + ".elassandra.username", "cassandra"),
                        environment.getProperty(scope + ".elassandra.password", "cassandra"))
                .withSSL(ssl().getSslOption())
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(environment.getProperty(scope + ".elassandra.connectTimeoutMillis", Integer.class, SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS))
                        .setReadTimeoutMillis(environment.getProperty(scope + ".elassandra.readTimeoutMillis", Integer.class, SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS)))
                .withQueryOptions(new QueryOptions()
                        .setConsistencyLevel(ConsistencyLevel.valueOf(environment.getProperty(scope + ".elassandra.consistencyLevel", ConsistencyLevel.LOCAL_QUORUM.name()))))
                .withLoadBalancingPolicy(lbp)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(environment.getProperty(scope + ".elassandra.reconnectDelay", Long.class, 5000L)));
        
        String addressTranslatorClassname = environment.getProperty(scope + ".elassandra.addressTranslator");
        if (addressTranslatorClassname != null) {
        	try {
        		LOGGER.info("Add cassandra addressTranslator={}", addressTranslatorClassname);
        		Class c = Class.forName("addressTranslatorClassname");
        	    builder.withAddressTranslator((AddressTranslator)c.newInstance());
        	} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        	    LOGGER.warn("Cannot instanciate addressTranslator="+addressTranslatorClassname, e);
        	}
        }
                
        return builder.build();
    }

    // keeps config for ElassandraCrud (environment not populated).
    public static class Config {
        String contactPoint;
        int    port;
        String endpoint;
        String username;
        String password;
        String indexPrefix;

        public String getContactPoint() {
            return contactPoint;
        }
        public int getPort() {
            return port;
        }
        public String getEndpoint() {
            return endpoint;
        }
        public String getUsername() {
            return username;
        }
        public String getPassword() {
            return password;
        }
        public String getIndexPrefix() {
            return indexPrefix;
        }
        public String toString() {
            return "contactPoint="+contactPoint+", port="+port+", endpoint="+endpoint+", username="+username+", indexPrefix="+indexPrefix;
        }
    }

    @Bean
    public Config config() {
        Config config = new Config();
        config.contactPoint = environment.getProperty(scope + ".elassandra.contactPoint", "localhost");
        config.port = environment.getProperty(scope + ".elassandra.port", Integer.class, 9042);
        config.endpoint = environment.getProperty(scope + ".elassandra.endpoint", "http://localhost:9200");
        config.username = environment.getProperty(scope + ".elassandra.username", "cassandra");
        config.password = environment.getProperty(scope + ".elassandra.password", "cassandra");
        config.indexPrefix = environment.getProperty(scope + ".elassandra.index.prefix", "");
        return config;
    }

    public static class Ssl {
        SSLContext sslContext;
        SSLOptions sslOption;

        public SSLContext getSslContext() {
            return sslContext;
        }
        public SSLOptions getSslOption() {
            return sslOption;
        }
    }

    @Bean
    public Ssl ssl() {
        Ssl ssl = new Ssl();

        String sslProviderName = environment.getProperty(scope + ".elassandra.ssl.provider", SslProvider.JDK.toString());
        String trustStorePath = environment.getProperty(scope + ".elassandra.ssl.truststore.path");
        String trustStorePass = environment.getProperty(scope + ".elassandra.ssl.truststore.password");
        String keyStorePath = environment.getProperty(scope + ".elassandra.ssl.keystore.path");
        String keyStorePass = environment.getProperty(scope + ".elassandra.ssl.keystore.password");

        LOGGER.info("Init security context, provider={} trustStorePath={} trustStorePass={}", sslProviderName, trustStorePath, trustStorePass);
        if (trustStorePath != null && trustStorePath.length() > 0) {
            LOGGER.info("Loading trustStorePath={}", trustStorePath);

            SslProvider sslProvider = SslProvider.valueOf(sslProviderName);
            final SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
            sslContextBuilder.sslProvider(sslProvider);
            final SSLContextBuilder sslBuilder = SSLContexts.custom();

            try {
                InputStream is = new FileInputStream(trustStorePath);
                KeyStore ks = KeyStore.getInstance(trustStorePath.endsWith(".jks") ? "JKS" : "PKCS12");
                ks.load(is, trustStorePass.toCharArray());
                sslBuilder.loadTrustMaterial(ks, new TrustStrategy() {
                    @Override
                    public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                        return true;
                    }
                });
                TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
                tmf.init(ks);
                sslContextBuilder.trustManager(tmf);
                LOGGER.info("Trust store {} sucessfully loaded.", trustStorePath);
            } catch(IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
                LOGGER.error("Failed to load trustStore="+ trustStorePath, e);
            }

            if (keyStorePath != null && keyStorePath.length() > 0) {
                Path keystorePath = get(keyStorePath);
                if (!Files.notExists(keystorePath)) {
                    try {
                        String keyStoreType = keyStorePath.endsWith(".jks") ? "JKS" : "PKCS12";
                        InputStream ksf = Files.newInputStream(keystorePath);
                        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                        KeyStore ks = KeyStore.getInstance(keyStoreType);
                        ks.load(ksf, keyStorePass.toCharArray());
                        for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); ) {
                            String alias = aliases.nextElement();
                            if (ks.getCertificate(alias).getType().equals("X.509")) {
                                Date expires = ((X509Certificate) ks.getCertificate(alias)).getNotAfter();
                                if (expires.before(new Date()))
                                    System.out.println("Certificate for " + alias + " expired on " + expires);
                            }
                        }
                        kmf.init(ks, keyStorePass.toCharArray());
                        sslContextBuilder.keyManager(kmf);
                        LOGGER.info("Keystore {} succefully loaded.", keystorePath);
                    } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException e) {
                        LOGGER.error("Failed to load keystore " + keystorePath, e);
                    }
                }
            }

            try {
                ssl.sslContext = sslBuilder.build();
                switch(sslProvider){
                    case JDK:
                        ssl.sslOption = RemoteEndpointAwareJdkSSLOptions.builder()
                            .withSSLContext(sslContext)
                            .build();
                    case OPENSSL:
                        ssl.sslOption = new RemoteEndpointAwareNettySSLOptions(sslContextBuilder.build());
                }
            } catch (SSLException | NoSuchAlgorithmException | KeyManagementException e) {
                LOGGER.error("Failed to build SSL context", e);
            }
        }
        return ssl;
    }

    /**
     * Create a session from the current Cassandra Cluster. Session will query in the defined keyspace.
     * @return Cassandra Session object
     */
    @Bean(destroyMethod = "close")
    public Session session() {
        String ks = environment.getProperty(scope + ".elassandra.keyspaceName", scope + "gravitee");
        LOGGER.debug("Creating Cassandra Session for the cluster=" + cluster().getClusterName()+ " keyspace="+ks);
        Session session = cluster().connect();
        String dc = session.getState().getConnectedHosts().iterator().next().getDatacenter();
        session.execute(String.format(Locale.ROOT, "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': '1' };", ks, dc));
        session.execute(String.format(Locale.ROOT, "USE %s", ks));
        LOGGER.info("Connected to cluster={} in datacenter={} keyspace={}", cluster().getClusterName(), dc, session.getLoggedKeyspace());
        return session;
    }

    /**
     * The repository does not use transactional workflow
     * @return transaction manager that does nothing
     */
    @Bean
    public AbstractPlatformTransactionManager graviteeTransactionManager() {
        return new NoTransactionManager();
    }

}
