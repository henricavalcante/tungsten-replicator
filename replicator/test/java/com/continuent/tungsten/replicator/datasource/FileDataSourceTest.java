/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
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
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.datasource;

import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.csv.CsvSpecification;
import com.continuent.tungsten.replicator.datasource.FileDataSource;

/**
 * Implements a test on file data source operations.
 */
public class FileDataSourceTest extends AbstractDataSourceTest
{
    /**
     * Set up properties used to configure the data source.
     */
    @Before
    public void setUp() throws Exception
    {
        // Create the data source definition.
        datasourceProps = new TungstenProperties();
        datasourceProps.setString("serviceName", "sqlcatalog");
        datasourceProps.setLong("channels", 10);
        datasourceProps.setString("directory", "fileCatalogTest");
        datasourceProps.setString("csv", CsvSpecification.class.getName());
        datasourceProps.setString("csv.fieldSeparator", "\t");
        datasourceProps.setBeanSupportEnabled(true);

        // Set the data source class.
        datasourceClass = FileDataSource.class.getName();
    }
}