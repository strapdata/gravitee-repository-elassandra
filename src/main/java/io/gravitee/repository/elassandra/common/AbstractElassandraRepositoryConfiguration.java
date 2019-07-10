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
        LOGGER.debug("Building Cassandra Cluster object");
        return Cluster.builder()
                .addContactPoints(environment.getProperty(scope + ".cassandra.contactPoint", "localhost"))
                .withPort(environment.getProperty(scope + ".cassandra.port", Integer.class, 9042))
                .withClusterName(environment.getProperty(scope + ".cassandra.clusterName", "elassandra"))
                .withCredentials(
                        environment.getProperty(scope + ".cassandra.username", "cassandra"),
                        environment.getProperty(scope + ".cassandra.password", "cassandra"))
                .withSSL(ssl().getSslOption())
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(environment.getProperty(scope + ".cassandra.connectTimeoutMillis", Integer.class, SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS))
                        .setReadTimeoutMillis(environment.getProperty(scope + ".cassandra.readTimeoutMillis", Integer.class, SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS)))
                .withQueryOptions(new QueryOptions()
                        .setConsistencyLevel(ConsistencyLevel.valueOf(environment.getProperty(scope + ".cassandra.consistencyLevel", QueryOptions.DEFAULT_CONSISTENCY_LEVEL.name()))))
                .build();
    }

    // keeps config for ElassandraCrud (environment not populated).
    public static class Config {
        String contactPoint;
        String endpoint;
        String username;
        String password;

        public String getContactPoint() {
            return contactPoint;
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
    }

    @Bean
    public Config config() {
        Config config = new Config();
        config.contactPoint = environment.getProperty(scope + ".cassandra.contactPoint", "localhost");
        config.endpoint = environment.getProperty(scope + ".cassandra.endpoint", "http://localhost:9200");
        config.username = environment.getProperty(scope + ".cassandra.username", "cassandra");
        config.password = environment.getProperty(scope + ".cassandra.password", "cassandra");
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

        String sslProviderName = environment.getProperty(scope + ".cassandra.ssl.provider", SslProvider.JDK.toString());
        String trustStorePath = environment.getProperty(scope + ".cassandra.ssl.truststore.path");
        String trustStorePass = environment.getProperty(scope + ".cassandra.ssl.truststore.password");
        String keyStorePath = environment.getProperty(scope + ".cassandra.ssl.keystore.path");
        String keyStorePass = environment.getProperty(scope + ".cassandra.ssl.keystore.password");

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
        String ks = environment.getProperty(scope + ".cassandra.keyspaceName", scope + "gravitee");
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
