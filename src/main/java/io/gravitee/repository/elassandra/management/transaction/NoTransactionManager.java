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
package io.gravitee.repository.elassandra.management.transaction;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class NoTransactionManager extends AbstractPlatformTransactionManager {

    private static final long serialVersionUID = 1L;

    @Override
    protected Object doGetTransaction() throws TransactionException {
        return new Object();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        //noop
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        //noop
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        //noop
    }
}
