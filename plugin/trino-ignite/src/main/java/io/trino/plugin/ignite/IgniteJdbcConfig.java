/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.ignite;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.IgniteJdbcThinDriver;

import java.sql.SQLException;
import java.util.Optional;

import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.URL_PREFIX;

public class IgniteJdbcConfig
        extends BaseJdbcConfig
{
    private static final String IDENTIFIER_QUOTE = "identifier-quote";
    private static final String DEFAULT_IDENTIFIER_QUOTE = "`";

    private Optional<String> identifierQuote = Optional.empty();

    @NotNull
    public String getIdentifierQuote()
    {
        return identifierQuote.orElse(DEFAULT_IDENTIFIER_QUOTE);
    }

    @Config(IDENTIFIER_QUOTE)
    @ConfigDescription("Quote character for query identifiers")
    public BaseJdbcConfig setIdentifierQuote(String identifierQuote)
    {
        this.identifierQuote = Optional.of(identifierQuote);
        return this;
    }

    @AssertTrue(message = "JDBC URL for Ignite connector should start with " + URL_PREFIX)
    public boolean isUrlValid()
    {
        try {
            return new IgniteJdbcThinDriver().acceptsURL(getConnectionUrl());
        }
        catch (SQLException e) {
            return false;
        }
    }
}
