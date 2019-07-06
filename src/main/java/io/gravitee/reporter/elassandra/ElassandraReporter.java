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
package io.gravitee.reporter.elassandra;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import io.gravitee.common.service.AbstractService;
import io.gravitee.elasticsearch.client.Client;
import io.gravitee.reporter.api.Reportable;
import io.gravitee.reporter.api.Reporter;
import io.gravitee.reporter.api.health.EndpointStatus;
import io.gravitee.reporter.api.http.Metrics;
import io.gravitee.reporter.api.log.Log;
import io.gravitee.reporter.api.monitor.Monitor;
import io.gravitee.reporter.elasticsearch.config.ReporterConfiguration;
import io.gravitee.reporter.elasticsearch.indexer.Indexer;
import io.gravitee.reporter.elasticsearch.mapping.IndexPreparer;
import io.gravitee.reporter.elasticsearch.spring.context.Elastic6xBeanRegistrer;
import io.reactivex.BackpressureStrategy;
import io.reactivex.CompletableObserver;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;

public class ElassandraReporter extends AbstractService implements Reporter {

    private final Logger logger = LoggerFactory.getLogger(ElassandraReporter.class);

    @Autowired
    private Client client;

    @Autowired
    private ReporterConfiguration configuration;

    /**
     * Indexer is settled in a lazy way as soon as the ES version has been discovered.
     */
    private Indexer indexer;

    @Override
    protected void doStart() throws Exception {
        if (configuration.isEnabled()) {
            super.doStart();

            logger.info("Starting Elassandra reporter engine...");

            // Wait for a connection to ES and retry each 5 seconds
            Single<Integer> singleVersion = client.getVersion()
                    .retryWhen(error -> error.flatMap(
                            throwable -> Observable.just(new Object()).delay(5, TimeUnit.SECONDS).toFlowable(BackpressureStrategy.LATEST)));

            singleVersion.subscribe();

            Integer version = singleVersion.blockingGet();

            boolean registered = true;

            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getAutowireCapableBeanFactory();

            switch (version) {
                case 6:
                    new Elastic6xBeanRegistrer().register(beanFactory);
                    break;
                default:
                    registered = false;
                    logger.error("Version {} is not supported by this Elassandra connector", version);
            }

            if (registered) {
                IndexPreparer preparer = applicationContext.getBean(IndexPreparer.class);
                preparer
                        .prepare()
                        .doOnComplete(() -> {
                            logger.info("Starting Elassandra reporter engine... DONE");
                        })
                        .subscribe(new CompletableObserver() {
                            @Override
                            public void onSubscribe(Disposable d) {}

                            @Override
                            public void onComplete() {
                                logger.info("Index mapping template successfully defined");
                            }

                            @Override
                            public void onError(Throwable t) {
                                logger.error("An error occurs while creating index mapping template", t);
                            }
                        });

                indexer = applicationContext.getBean(Indexer.class);
            } else {
                logger.info("Starting Elassandra reporter engine... ERROR");
            }
        }
    }

    @Override
    public void report(Reportable reportable) {
        if (configuration.isEnabled()) {
            indexer.index(reportable);
        }
    }

    Single rxReport(Reportable reportable) {
        indexer.index(reportable);
        return Single.just(reportable);
    }

    @Override
    public boolean canHandle(Reportable reportable) {
        return reportable instanceof Metrics ||
                reportable instanceof EndpointStatus ||
                reportable instanceof Monitor ||
                reportable instanceof Log;
    }

    @Override
    protected void doStop() throws Exception {
        if (configuration.isEnabled()) {
            super.doStop();

            logger.info("Stopping Elassandra reporter engine... DONE");
        }
    }
}
