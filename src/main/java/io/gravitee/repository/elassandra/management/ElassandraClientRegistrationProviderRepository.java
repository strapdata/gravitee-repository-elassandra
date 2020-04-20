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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.Row;

import io.gravitee.repository.management.api.ClientRegistrationProviderRepository;
import io.gravitee.repository.management.model.ClientRegistrationProvider;
import io.gravitee.repository.management.model.ClientRegistrationProvider.InitialAccessTokenType;

@Repository
public class ElassandraClientRegistrationProviderRepository extends ElassandraCrud<ClientRegistrationProvider, String> implements ClientRegistrationProviderRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElassandraClientRegistrationProviderRepository.class);

    public ElassandraClientRegistrationProviderRepository() {
        super("client_registration_providers",
                new String[] { "id", "name", "description", "discovery_endpoint", "initial_access_token_type", "initial_access_token", "client_id", "client_secret", "scopes", "renew_client_secret_support", "renew_client_secret_endpoint", "renew_client_secret_method", "createdAt", "updatedAt"},
                new String[] {"text", "text", "text",       "text",               "text",                      "text",                 "text",      "text",          "list<text>", "boolean",                 "text",                         "text",                       "timestamp",      "timestamp"},
                1, 1);
    }

    @Override
    public ClientRegistrationProvider fromRow(Row row) {
        if (row != null) {
            final ClientRegistrationProvider clientRegistrationProvider = new ClientRegistrationProvider();
            clientRegistrationProvider.setId(row.getString("id"));
            clientRegistrationProvider.setName(row.getString("name"));
            clientRegistrationProvider.setDescription(row.getString("description"));
            clientRegistrationProvider.setDiscoveryEndpoint(row.getString("discovery_endpoint"));
            clientRegistrationProvider.setInitialAccessTokenType(InitialAccessTokenType.valueOf(row.getString("initial_access_token_type")));
            clientRegistrationProvider.setInitialAccessToken(row.getString("initial_access_token"));
            clientRegistrationProvider.setClientId(row.getString("client_id"));
            clientRegistrationProvider.setClientSecret(row.getString("client_secret"));
            clientRegistrationProvider.setScopes(row.getList("scopes", String.class));
            clientRegistrationProvider.setRenewClientSecretSupport(row.isNull("renew_client_secret_support") ? null : row.getBool("renew_client_secret_support"));
            clientRegistrationProvider.setRenewClientSecretEndpoint(row.getString("renew_client_secret_endpoint"));
            clientRegistrationProvider.setRenewClientSecretMethod(row.getString("renew_client_secret_method"));
            clientRegistrationProvider.setCreatedAt(row.getTimestamp("createdAt"));
            clientRegistrationProvider.setUpdatedAt(row.getTimestamp("updatedAt"));
            return clientRegistrationProvider;
        }
        return null;
    }

    @Override
    public Object[] values(ClientRegistrationProvider t) {
        return new Object[] {
                t.getId(),
                t.getName(),
                t.getDescription(),
                t.getDiscoveryEndpoint(),
                t.getInitialAccessTokenType() == null ? null : t.getInitialAccessTokenType().toString(),
                t.getInitialAccessToken(),
                t.getClientId(),
                t.getClientSecret(),
                t.getScopes(),
                t.isRenewClientSecretSupport(),
                t.getRenewClientSecretEndpoint(),
                t.getRenewClientSecretMethod(),
                t.getCreatedAt(),
                t.getUpdatedAt()
        };
    }

}
