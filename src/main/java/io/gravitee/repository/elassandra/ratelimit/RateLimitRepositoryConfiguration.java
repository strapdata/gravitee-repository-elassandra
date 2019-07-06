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
package io.gravitee.repository.elassandra.ratelimit;

import io.gravitee.repository.Scope;
import io.gravitee.repository.elassandra.common.AbstractElassandraRepositoryConfiguration;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Configuration
@ComponentScan
@Profile("!test")
public class RateLimitRepositoryConfiguration extends AbstractElassandraRepositoryConfiguration {
    @Override
    protected Scope getScope() {
        return Scope.RATE_LIMIT;
    }
}
