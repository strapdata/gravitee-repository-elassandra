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

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.management.api.search.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public interface PageableRepository<T> {

    public static final Logger LOGGER = LoggerFactory.getLogger(PageableRepository.class);

    default Page<T> getResultAsPage(final Pageable page, final List<T> items) {
        if (page != null) {
            LOGGER.debug("Getting results as page {} for {}", page, items);
            int start = page.from();
            if ((start == 0) && (page.pageNumber() > 0)) {
                start = page.pageNumber() * page.pageSize();
            }
            int rows = page.pageSize();
            if ((rows == 0) && (page.to() > 0)) {
                rows = page.to() - start;
            }
            if (start + rows > items.size()) {
                rows = items.size() - start;
            }
            return new Page<T>(items.subList(start, start + rows), start / page.pageSize(), rows, items.size());
        }
        return new Page<T>(items, 0, items.size(), items.size());
    }
}
