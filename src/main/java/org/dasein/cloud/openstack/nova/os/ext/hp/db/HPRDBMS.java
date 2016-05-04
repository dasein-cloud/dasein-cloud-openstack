/**
 * Copyright (C) 2009-2015 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.openstack.nova.os.ext.hp.db;

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.*;
import org.dasein.cloud.util.APITrace;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Implements Dasein Cloud relational database support for the HP cloud.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.04.1
 * @version 2012.04.1
 * @version updated for 2013.02 model
 */
public class HPRDBMS extends AbstractRelationalDatabaseSupport<NovaOpenStack> {
    static private final Logger logger = NovaOpenStack.getLogger(HPRDBMS.class, "std");

    static public final String RESOURCE  = "/instances";
    static public final String SNAPSHOTS = "/snapshots";
    
    static public final String SERVICE  = "hpext:database";
    
    public HPRDBMS(NovaOpenStack provider) { super(provider); }

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return getContext().getAccountNumber();
    }

    @Override
    public @Nonnull String createFromScratch(@Nonnull String dataSourceName, @Nonnull DatabaseProduct product, @Nonnull String databaseVersion, @Nonnull String withAdminUser, @Nonnull String withAdminPassword, int hostPort) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.createFromScratch");
        try {
            Map<String,Object> wrapper = new HashMap<>();
            Map<String,Object> json = new HashMap<>();
            NovaMethod method = new NovaMethod(getProvider());

            json.put("flavorRef", getFlavorRef(product.getProductSize()));
   
            json.put("name", dataSourceName);
            json.put("port", hostPort > 0 ? hostPort : 3306);
            if( product.getEngine().equals(DatabaseEngine.MYSQL) ) {
                Map<String,Object> type = new HashMap<>();
                
                type.put("name", "mysql");
                if( databaseVersion != null ) {
                    type.put("version", databaseVersion);
                }
                //no longer engine differentiation on MYSQL versions
                /*else if( product.getEngine().equals(DatabaseEngine.MYSQL51) ) {
                    type.put("version", "5.1");
                }
                else if( product.getEngine().equals(DatabaseEngine.MYSQL50) ) {
                    type.put("version", "5.0");
                } */
                else {
                    type.put("version", "5.5");
                }
                json.put("dbtype", type);
            }
            else {
                throw new InternalException("Unsupported database product: " + product);
            }
            wrapper.put("instance", json);
            JSONObject result = method.postString(SERVICE, RESOURCE, null, new JSONObject(wrapper), true);

            if( result != null && result.has("instance") ) {
                try {
                    Database db = toDatabase(result.getJSONObject("instance"));

                    if( db != null ) {
                        return db.getProviderDatabaseId();
                    }
                }
                catch( JSONException e ) {
                    logger.error("createFromScratch(): Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand create response: " + e.getMessage(), e);
                }
            }
            logger.error("createFromScratch(): No database was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No database was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String createFromLatest(String dataSourceName, String providerDatabaseId, String productSize, String providerDataCenterId, int hostPort) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "RDBMS.createFromLatest");
        try {
            DatabaseSnapshot snapshot = null;
            
            for( DatabaseSnapshot s : listSnapshots(providerDatabaseId) ) {
                if( snapshot == null || s.getSnapshotTimestamp() > snapshot.getSnapshotTimestamp() ) {
                    snapshot = s;
                }
            }
            if( snapshot == null ) {
                throw new InternalException("No snapshots exist from which to create a new database instance");
            }
            return createFromSnapshot(dataSourceName, providerDatabaseId, snapshot.getProviderSnapshotId(), productSize, providerDataCenterId, hostPort);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String createFromSnapshot(String dataSourceName, String providerDatabaseId, String providerDbSnapshotId, String productSize, String providerDataCenterId, int hostPort) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.createFromSnapshot");
        try {
            Map<String,Object> wrapper = new HashMap<>();
            Map<String,Object> json = new HashMap<>();
            NovaMethod method = new NovaMethod(getProvider());

            json.put("flavorRef", getFlavorRef(productSize));

            json.put("name", dataSourceName);
            json.put("port", hostPort > 0 ? hostPort : 3306);
            json.put("snapshotId", providerDbSnapshotId);

            wrapper.put("instance", json);
            JSONObject result = method.postString(SERVICE, RESOURCE, null, new JSONObject(wrapper), true);

            if( result != null && result.has("instance") ) {
                try {
                    Database db = toDatabase(result.getJSONObject("instance"));

                    if( db != null ) {
                        return db.getProviderDatabaseId();
                    }
                }
                catch( JSONException e ) {
                    logger.error("createFromSnapshot(): Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand create response: " + e.getMessage(), e);
                }
            }
            logger.error("createFromSnapshot(): No database was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No database was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public String createFromTimestamp(String dataSourceName, String providerDatabaseId, long beforeTimestamp, String productSize, String providerDataCenterId, int hostPort) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "RDBMS.createFromTimestamp");
        try {
            DatabaseSnapshot snapshot = null;

            for( DatabaseSnapshot s : listSnapshots(providerDatabaseId) ) {
                if( s.getSnapshotTimestamp() < beforeTimestamp && (snapshot == null || s.getSnapshotTimestamp() > snapshot.getSnapshotTimestamp()) ) {
                    snapshot = s;
                }
            }
            if( snapshot == null ) {
                throw new InternalException("No snapshots exist from which to create a new database instance");
            }
            return createFromSnapshot(dataSourceName, providerDatabaseId, snapshot.getProviderSnapshotId(), productSize, providerDataCenterId, hostPort);
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile HPRDBMSCapabilities capabilities;
    @Nonnull
    @Override
    public RelationalDatabaseCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new HPRDBMSCapabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    public @Nullable DatabaseConfiguration getConfiguration(@Nonnull String providerConfigurationId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nullable Database getDatabase(@Nonnull String providerDatabaseId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.getDatabase");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            JSONObject ob = method.getResource(SERVICE, RESOURCE, providerDatabaseId, false);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("instance") ) {
                    return toDatabase(ob.getJSONObject("instance"));
                }
            }
            catch( JSONException e ) {
                logger.error("getDatabase(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to understand create response: " + e.getMessage(), e);
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<DatabaseEngine> getDatabaseEngines() throws CloudException, InternalException {
        return Collections.singletonList(DatabaseEngine.MYSQL);
    }

    @Override
    public @Nullable String getDefaultVersion(@Nonnull DatabaseEngine forEngine) throws CloudException, InternalException {
        if( forEngine.equals(DatabaseEngine.MYSQL) ) {
            return "5.5";
        }
        return null;
    }

    @Override
    public Iterable<String> getSupportedVersions(DatabaseEngine forEngine) throws CloudException, InternalException {
        if( forEngine.equals(DatabaseEngine.MYSQL) ) {
            return Collections.singletonList("5.5");
        }
        return Collections.emptyList();
    }

    public @Nullable DatabaseProduct getDatabaseProduct(String flavor) throws CloudException, InternalException {
        boolean hasSize = flavor.contains(":");

        for( DatabaseEngine engine : DatabaseEngine.values() ) {
            for( DatabaseProduct product : getDatabaseProducts(engine) ) {
                if( hasSize && product.getProductSize().equals(flavor) ) {
                    return product;
                }
                else if( !hasSize && product.getProductSize().startsWith(flavor) ) {
                    return product;
                }
            }
        }
        return null;
    }

    public Iterable<DatabaseProduct> getDatabaseProducts(DatabaseEngine forEngine) throws CloudException, InternalException {
        if( DatabaseEngine.MYSQL.equals(forEngine) ) {
            Logger std = NovaOpenStack.getLogger(HPRDBMS.class, "std");

            if( std.isTraceEnabled() ) {
                std.trace("ENTER: " + HPRDBMS.class.getName() + ".getDatabaseProducts()");
            }
            try {
                NovaMethod method = new NovaMethod(getProvider());

                JSONObject json = method.getResource(SERVICE, "/flavors", null, false);

                List<DatabaseProduct> products = new ArrayList<>();

                if( json != null && json.has("flavors") ) {
                    try {
                        JSONArray flavors = json.getJSONArray("flavors");

                        for( int i=0; i<flavors.length(); i++ ) {
                            JSONObject flavor = flavors.getJSONObject(i);

                            if( flavor != null ) {
                                for( int size : new int[] { 2, 5, 10, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 400, 500, 600, 700, 800, 900, 1000 } ) {
                                    DatabaseProduct product = toProduct(size, flavor);

                                    if( product != null ) {
                                        products.add(product);
                                    }
                                }
                            }
                        }
                    }
                    catch( JSONException e ) {
                        std.error("getDatabaseProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                        throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
                    }
                }
                return products;
            }
            finally {
                if( std.isTraceEnabled() ) {
                    std.trace("exit - " + HPRDBMS.class.getName() + ".getDatabaseProducts()");
                }
            }
        }
        else {
            return Collections.emptyList();
        }
    }

    @Override
    public Iterable<DatabaseProduct> listDatabaseProducts(DatabaseEngine databaseEngine) throws CloudException, InternalException {
        if( DatabaseEngine.MYSQL.equals(databaseEngine) ) {
            if( logger.isTraceEnabled() ) {
                logger.trace("ENTER: " + HPRDBMS.class.getName() + ".getDatabaseProducts()");
            }
            try {
                NovaMethod method = new NovaMethod(getProvider());

                JSONObject json = method.getResource(SERVICE, "/flavors", null, false);

                List<DatabaseProduct> products = new ArrayList<>();

                if( json != null && json.has("flavors") ) {
                    try {
                        JSONArray flavors = json.getJSONArray("flavors");

                        for( int i=0; i<flavors.length(); i++ ) {
                            JSONObject flavor = flavors.getJSONObject(i);

                            if( flavor != null ) {
                                for( int size : new int[] { 2, 5, 10, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 400, 500, 600, 700, 800, 900, 1000 } ) {
                                    DatabaseProduct product = toProduct(size, flavor);

                                    if( product != null ) {
                                        products.add(product);
                                    }
                                }
                            }
                        }
                    }
                    catch( JSONException e ) {
                        logger.error("getDatabaseProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                        throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
                    }
                }
                return products;
            }
            finally {
                if( logger.isTraceEnabled() ) {
                    logger.trace("exit - " + HPRDBMS.class.getName() + ".getDatabaseProducts()");
                }
            }
        }
        else {
            return Collections.emptyList();
        }
    }

    private @Nullable String getFlavorRef(@Nonnull String productId) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + HPRDBMS.class.getName() + ".getFlavorRef(" + productId + ")");
        }
        try {
            int idx = productId.indexOf(":");

            if( idx > -1 ) {
                productId = productId.substring(0, idx);
            }
            NovaMethod method = new NovaMethod(getProvider());

            JSONObject json = method.getResource(SERVICE, "/flavors", productId, false);

            if( json != null && json.has("flavor") ) {
                try {
                    JSONObject flavor = json.getJSONObject("flavor");

                    if( flavor.has("links") ) {
                        JSONArray links = flavor.getJSONArray("links");

                        if( links != null ) {
                            for( int i=0; i<links.length(); i++ ) {
                                JSONObject link = links.getJSONObject(i);

                                if( link.has("rel") ) {
                                    String rel = link.getString("rel");

                                    if( rel != null && rel.equalsIgnoreCase("self") ) {
                                        return link.getString("href");
                                    }
                                }
                            }
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("getFlavorRef(): Unable to identify expected values in JSON: " + e.getMessage());
                    throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
                }
            }
            return null;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + HPRDBMS.class.getName() + ".getFlavorRef()");
            }
        }
    }

    @Override
    public DatabaseSnapshot getSnapshot(String providerDbSnapshotId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.getSnapshot");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            JSONObject ob = method.getResource(SERVICE, SNAPSHOTS, providerDbSnapshotId, false);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("snapshot") ) {
                    return toSnapshot(ob.getJSONObject("snapshot"));
                }
            }
            catch( JSONException e ) {
                logger.error("getSnapshot(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.isSubscribed");
        try {
            return (getProvider().getAuthenticationContext().getServiceUrl(SERVICE) != null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<String> listAccess(String toProviderDatabaseId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public Iterable<DatabaseConfiguration> listConfigurations() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listDatabaseStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.listDatabaseStatus");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            List<ResourceStatus> databases = new ArrayList<>();

            JSONObject json = method.getResource(SERVICE, RESOURCE, null, false);

            if( json != null && json.has("instances") ) {
                try {
                    JSONArray list = json.getJSONArray("instances");

                    for( int i=0; i<list.length(); i++ ) {
                        ResourceStatus db = toStatus(list.getJSONObject(i));

                        if( db != null ) {
                            databases.add(db);
                        }
                    }
                }
                catch( JSONException e ) {
                    throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
                }
            }
            return databases;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<Database> listDatabases() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.listDatabases");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            List<Database> databases = new ArrayList<>();

            JSONObject json = method.getResource(SERVICE, RESOURCE, null, false);

            if( json != null && json.has("instances") ) {
                try {
                    JSONArray list = json.getJSONArray("instances");

                    for( int i=0; i<list.length(); i++ ) {
                        Database db = toDatabase(list.getJSONObject(i));

                        if( db != null ) {
                            databases.add(db);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("listDatabases(): Unable to identify expected values in JSON: " + e.getMessage());
                    throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
                }
            }
            return databases;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<ConfigurationParameter> listParameters(String forProviderConfigurationId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public Iterable<DatabaseSnapshot> listSnapshots(String forOptionalProviderDatabaseId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.listSnapshots");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            List<DatabaseSnapshot> snapshots = new ArrayList<DatabaseSnapshot>();

            JSONObject json = method.getResource(SERVICE, SNAPSHOTS, null, false);

            if( json != null && json.has("snapshots") ) {
                try {
                    JSONArray list = json.getJSONArray("snapshots");

                    for( int i=0; i<list.length(); i++ ) {
                        DatabaseSnapshot snapshot = toSnapshot(list.getJSONObject(i));

                        if( snapshot != null ) {
                            snapshots.add(snapshot);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("listSnapshots(): Unable to identify expected values in JSON: " + e.getMessage());
                    throw new CommunicationException("Unable to identify expected values in JSON: " + e.getMessage(), e);
                }
            }
            return snapshots;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeDatabase(String providerDatabaseId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.removeDatabase");
        try {
            NovaMethod method = new NovaMethod(getProvider());

            method.deleteResource(SERVICE, RESOURCE, providerDatabaseId, null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeSnapshot(String providerSnapshotId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.removeSnapshot");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            method.deleteResource(SERVICE, SNAPSHOTS, providerSnapshotId, null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void resetConfiguration(String providerConfigurationId, String... parameters) throws CloudException, InternalException {
        // NO-OP since all configurations are at their defaults without configuration support
    }

    @Override
    public void restart(String providerDatabaseId, boolean blockUntilDone) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.restart");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            method.postResourceHeaders(SERVICE, RESOURCE, providerDatabaseId + "/restart", new HashMap<String,String>());

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public DatabaseSnapshot snapshot(String providerDatabaseId, String name) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "RDBMS.snapshot");
        try {
            Map<String,Object> wrapper = new HashMap<>();
            Map<String,Object> json = new HashMap<>();
            NovaMethod method = new NovaMethod(getProvider());

            json.put("name", name);
            json.put("instanceId", providerDatabaseId);

            wrapper.put("snapshot", json);
            JSONObject result = method.postString(SERVICE, SNAPSHOTS, null, new JSONObject(wrapper), true);

            if( result != null && result.has("snapshot") ) {
                try {
                    DatabaseSnapshot snapshot = toSnapshot(result.getJSONObject("snapshot"));

                    if( snapshot != null ) {
                        return snapshot;
                    }
                }
                catch( JSONException e ) {
                    logger.error("snapshot(): Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Unable to understand create response: " + e.getMessage(), e);
                }
            }
            logger.error("snapshot(): No snapshot was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No snapshot was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    private @Nullable Database toDatabase(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        
        String regionId = getContext().getRegionId();

        try {
            String dbId = (json.has("id") ? json.getString("id") : null);
            
            if( dbId == null ) {
                return null;
            }
            
            String name= (json.has("name") ? json.getString("name") : null);
            
            if( name == null ) {
                name = "RDBMS MySQL #" + dbId;
            }
    
            DatabaseState currentState = DatabaseState.PENDING;
            String status = (json.has("status") ? json.getString("status") : null);

            if( status != null ) {
                if( status.equalsIgnoreCase("BUILD") || status.equalsIgnoreCase("building") ) {
                    currentState = DatabaseState.PENDING;
                }
                else if( status.equalsIgnoreCase("AVAILABLE") ) {
                    currentState = DatabaseState.AVAILABLE;
                }
                else if( status.equalsIgnoreCase("running") ) {
                    currentState = DatabaseState.AVAILABLE;
                }
                else {
                    logger.debug("DEBUG OS DB STATE: " + status);
                }
            }
            long created = (json.has("created") ? getProvider().parseTimestamp(json.getString("created")) : -1L);

            String hostname = (json.has("hostname") ? json.getString("hostname") : null);
            String user = null;
            
            if( json.has("credential") ) {
                JSONObject c = json.getJSONObject("credential");
                
                if( c.has("username") ) {
                    user = c.getString("username");
                }
            }
            String flavor = (json.has("flavorRef") ? json.getString("flavorRef") : null);

            if( flavor == null ) {
                JSONObject f = (json.has("flavor") ? json.getJSONObject("flavor") : null);

                if( f != null && f.has("id") ) {
                    flavor = f.getString("id");
                }
            }
            else {
                int idx = flavor.lastIndexOf("/");

                if( idx > -1 ) {
                    flavor = flavor.substring(idx+1);
                }
            }
            int port = (json.has("port") ? json.getInt("port") : 3306);
            DatabaseEngine engine = DatabaseEngine.MYSQL;
            
            if( json.has("dbtype") ) {
                JSONObject t = json.getJSONObject("dbtype");
                String db = "mysql", version = "5.5";
                
                if( t.has("name") ) {
                    db = t.getString("name");
                }
                if( t.has("version") ) {
                    version = t.getString("version");
                }
                if( db.equalsIgnoreCase("mysql") ) {
                    if( !version.startsWith("5.5") ) {
                        //no longer engine differentiation
                        /*if( version.startsWith("5.1") ) {
                            engine = DatabaseEngine.MYSQL;
                        }
                        else if( version.startsWith("5.0") ) {
                            engine = DatabaseEngine.MYSQL;
                        }
                        else {
                        */
                            logger.debug("DEBUG OS UNKNOWN MYSQL VERSION " + version);
                            engine = DatabaseEngine.MYSQL;
                       // }
                    }
                }
                else {
                    logger.debug("DEBUG OS UNKNOWN DB: " + db + " " + version);
                }
            }
            DatabaseProduct product = (flavor == null ? null : getDatabaseProduct(flavor));

            Database database = new Database();

            database.setAdminUser(user);
            database.setAllocatedStorageInGb(product == null ? 0 : product.getStorageInGigabytes());
            database.setCreationTimestamp(created);
            database.setCurrentState(currentState);
            database.setEngine(engine);
            database.setHighAvailability(false);
            database.setHostName(hostname);
            database.setHostPort(port);
            database.setName(name);
            database.setProductSize(flavor);
            database.setProviderDatabaseId(dbId);
            database.setProviderRegionId(getContext().getRegionId());
            database.setProviderDataCenterId(regionId + "-a");
            database.setProviderOwnerId(getTenantId());
            database.setProviderRegionId(regionId);
            return database;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to understand response: " + e.getMessage(), e);
        }
    }
         
    private @Nullable DatabaseSnapshot toSnapshot(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        
        try {
            String regionId = getContext().getRegionId();
            
            String snapshotId = (json.has("id") ? json.getString("id") : null);
            
            if( snapshotId == null ) {
                return null;
            }
            String dbId = (json.has("instanceId") ? json.getString("instanceId") : null);

            DatabaseSnapshotState currentState = DatabaseSnapshotState.CREATING;
            String status = (json.has("status") ? json.getString("status") : null);
            
            if( status != null ) {
                if( status.equalsIgnoreCase("building") ) {
                    currentState = DatabaseSnapshotState.CREATING;
                }
                else if( status.equalsIgnoreCase("available") ) {
                    currentState = DatabaseSnapshotState.AVAILABLE;
                }
                else if( status.equalsIgnoreCase("deleted") ) {
                    currentState = DatabaseSnapshotState.DELETED;
                }
                else {
                    logger.debug("DEBUG OS DBSNAP STATE: " + status);
                }
            }
            long created = (json.has("created") ? getProvider().parseTimestamp(json.getString("created")) : -1L);

            DatabaseSnapshot snapshot = new DatabaseSnapshot();
            
            snapshot.setProviderSnapshotId(snapshotId);
            snapshot.setProviderRegionId(regionId);
            snapshot.setCurrentState(currentState);
            snapshot.setProviderDatabaseId(dbId);
            snapshot.setProviderOwnerId(getTenantId());
            snapshot.setSnapshotTimestamp(created);
            snapshot.setStorageInGigabytes(0);
            snapshot.setAdminUser(null);
            return snapshot;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to understand  response: " + e.getMessage(), e);
        }
    }

    private @Nullable DatabaseProduct toProduct(@Nonnegative int size, @Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        try {
            String id = (json.has("id") ? json.getString("id") : null);

            if( id == null ) {
                return null;
            }

            String name = (json.has("name") ? json.getString("name") : null);

            if( name == null ) {
                name = id + " (" + size + " GB)";
            }
            else {
                name = name + " [" + size + " GB]";
            }
            id = id + ":" + size;

            String regionId = getContext().getRegionId();

            if( regionId == null ) {
                throw new InternalException("No region is associated with this request");
            }
            DatabaseProduct product = new DatabaseProduct(id, name);

            if( regionId.equals("LON") ) {
                product.setCurrency("GBP");
            }
            else {
                product.setCurrency("USD");
            }
            product.setEngine(DatabaseEngine.MYSQL);
            product.setHighAvailability(false);
            product.setProviderDataCenterId(regionId + "-1");
            product.setStandardHourlyRate(0.0f);
            product.setStandardIoRate(0.0f);
            product.setStandardStorageRate(0.0f);
            product.setStorageInGigabytes(size);

            return product;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to understand response: " + e.getMessage(), e);
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }

        try {
            String dbId = (json.has("id") ? json.getString("id") : null);

            if( dbId == null ) {
                return null;
            }

            DatabaseState currentState = DatabaseState.PENDING;
            String status = (json.has("status") ? json.getString("status") : null);

            if( status != null ) {
                if( status.equalsIgnoreCase("BUILD") || status.equalsIgnoreCase("building") ) {
                    currentState = DatabaseState.PENDING;
                }
                else if( status.equalsIgnoreCase("AVAILABLE") ) {
                    currentState = DatabaseState.AVAILABLE;
                }
                else if( status.equalsIgnoreCase("running") ) {
                    currentState = DatabaseState.AVAILABLE;
                }
                else {
                    logger.debug("DEBUG OS DB STATE: " + status);
                }
            }
            return new ResourceStatus(dbId, currentState);
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to understand response: " + e.getMessage(), e);
        }
    }
}
