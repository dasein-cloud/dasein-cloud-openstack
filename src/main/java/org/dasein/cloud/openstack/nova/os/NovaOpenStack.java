/**
 * Copyright (C) 2009-2014 Dell, Inc.
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

package org.dasein.cloud.openstack.nova.os;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.openstack.nova.os.compute.NovaComputeServices;
import org.dasein.cloud.openstack.nova.os.ext.hp.HPPlatformServices;
import org.dasein.cloud.openstack.nova.os.ext.rackspace.RackspacePlatformServices;
import org.dasein.cloud.openstack.nova.os.identity.NovaIdentityServices;
import org.dasein.cloud.openstack.nova.os.network.NovaNetworkServices;
import org.dasein.cloud.openstack.nova.os.storage.SwiftStorageServices;
import org.dasein.cloud.platform.PlatformServices;
import org.dasein.cloud.storage.StorageServices;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONObject;

public class NovaOpenStack extends AbstractCloud {
    static private final Logger logger = getLogger(NovaOpenStack.class, "std");

    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls, @Nonnull String type) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        if( pkg.equals("os") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.nova." + type + "." + pkg + getLastItem(cls.getName()));
    }

    static public boolean isSupported(@Nonnull String version) {
        int idx = version.indexOf('.');
        int major, minor;
        
        if( idx < 0 ) {
            major = Integer.parseInt(version);
            minor = 0;
        }
        else {
            String[] parts = version.split("\\.");
            
            major = Integer.parseInt(parts[0]);
            minor = Integer.parseInt(parts[1]);
        }
        return (major <= 2 && minor < 10);
    }
    
    public NovaOpenStack() { }
    
    public synchronized @Nonnull AuthenticationContext getAuthenticationContext() throws CloudException, InternalException {
        APITrace.begin(this, "Cloud.getAuthenticationContext");
        try {
            Cache<AuthenticationContext> cache = Cache.getInstance(this, "authenticationContext", AuthenticationContext.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                throw new InternalException("No context was set for this request");
            }
            Iterable<AuthenticationContext> current = cache.get(ctx);
            AuthenticationContext authenticationContext = null;

            NovaMethod method = new NovaMethod(this);

            if( current != null ) {
                authenticationContext = current.iterator().next();
            }
            else {
                try {
                    authenticationContext = method.authenticate();
                }
                finally {
                    if( authenticationContext == null ) {
                        NovaException.ExceptionItems items = new NovaException.ExceptionItems();

                        items.code = HttpStatus.SC_UNAUTHORIZED;
                        items.type = CloudErrorType.AUTHENTICATION;
                        items.message = "unauthorized";
                        items.details = "The API keys failed to authenticate with the specified endpoint.";
                        throw new NovaException(items);
                    }
                    cache.put(ctx, Collections.singletonList(authenticationContext));
                    return authenticationContext;
                }
            }
            return authenticationContext;
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getCloud().getCloudName());

        return (name != null ? name : "OpenStack");
    }

    @Override
    public @Nonnull
    ContextRequirements getContextRequirements() {
        return new ContextRequirements(
                new ContextRequirements.Field("apiKey", "The API Keypair", ContextRequirements.FieldType.KEYPAIR, ContextRequirements.Field.ACCESS_KEYS, true)
        );
    }
    
    @Override
    public @Nonnull NovaComputeServices getComputeServices() {
        return new NovaComputeServices(this);
    }

    @Override
    public @Nullable NovaIdentityServices getIdentityServices() {
        return new NovaIdentityServices(this);
    }

    @Override
    public @Nonnull NovaLocationServices getDataCenterServices() {
        return new NovaLocationServices(this);
    }
    
    @Override
    public @Nullable PlatformServices getPlatformServices() {
        if( getCloudProvider().equals(OpenStackProvider.HP) ) {
            return new HPPlatformServices(this);
        }
        else if( getCloudProvider().equals(OpenStackProvider.RACKSPACE) ) {
            return new RackspacePlatformServices(this);
        }
        return super.getPlatformServices();
    }

    @Override
    public @Nullable StorageServices getStorageServices() {
        ProviderContext pc = getContext();

        if( pc == null ) {
            return null;
        }
        if( pc.getStorageEndpoint() == null ) {
            try {
                AuthenticationContext ctx = getAuthenticationContext();
                
                if( ctx.getStorageUrl() == null ) {
                    return null;
                }
            }
            catch( CloudException e ) {
                return null;
            }
            catch( InternalException e ) {
                return null;
            }
            return new SwiftStorageServices(this);
        }
        return new SwiftStorageServices(this);
    }
    
    
    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getCloud().getProviderName());
        
        return (name != null ? name : "OpenStack");
    }
    
    public @Nonnegative int getMajorVersion() throws CloudException, InternalException {
        AuthenticationContext ctx = getAuthenticationContext();
        String endpoint = ctx.getComputeUrl();
        
        if( endpoint == null ) {
            endpoint = ctx.getStorageUrl();
            if( endpoint == null ) {
                return 1;
            }
        }
        while( endpoint.endsWith("/") && endpoint.length() > 1 ) {
            endpoint = endpoint.substring(0,endpoint.length()-1);
        }
        String[] parts = endpoint.split("/");
        int idx = parts.length-1;
        
        do {
            endpoint = parts[idx];
            while( !Character.isDigit(endpoint.charAt(0)) && endpoint.length() > 1 ) {
                endpoint = endpoint.substring(1);
            }
            if( Character.isDigit(endpoint.charAt(0)) ) {
                int i = endpoint.indexOf('.');
                
                try {
                    if( i == -1 ) {
                        return Integer.parseInt(endpoint);
                    }
                    String[] d = endpoint.split("\\.");

                    return Integer.parseInt(d[0]);
                }
                catch( NumberFormatException ignore ) {
                    // ignore
                }
            }
        } while( (idx--) > 0 );
        return 1;
    }
    
    public @Nonnegative int getMinorVersion() throws CloudException, InternalException {
        AuthenticationContext ctx = getAuthenticationContext();
        String endpoint = ctx.getComputeUrl();
        
        if( endpoint == null ) {
            endpoint = ctx.getStorageUrl();
            if( endpoint == null ) {
                return 1;
            }
        }
        while( endpoint.endsWith("/") && endpoint.length() > 1 ) {
            endpoint = endpoint.substring(0,endpoint.length()-1);
        }
        String[] parts = endpoint.split("/");
        int idx = parts.length-1;
        
        do {
            endpoint = parts[idx];
            while( !Character.isDigit(endpoint.charAt(0)) && endpoint.length() > 1 ) {
                endpoint = endpoint.substring(1);
            }
            if( Character.isDigit(endpoint.charAt(0)) ) {
                int i = endpoint.indexOf('.');
                
                try {
                    if( i == -1 ) {
                        return Integer.parseInt(endpoint);
                    }
                    String[] d = endpoint.split("\\.");

                    return Integer.parseInt(d[1]);
                }
                catch( NumberFormatException ignore ) {
                    // ignore
                }
            }
        } while( (idx--) > 0 );
        return 1;
    }

    @Override
    public @Nullable NovaNetworkServices getNetworkServices() {
        return new NovaNetworkServices(this);
    }

    public boolean isHP() {
        return getProviderName().equalsIgnoreCase("hp");
    }

    public boolean isInsecure() {
        ProviderContext ctx = getContext();
        String value;

        if( ctx == null ) {
            value = null;
        }
        else {
            Properties p = ctx.getCustomProperties();

            if( p == null ) {
                value = null;
            }
            else {
                value = p.getProperty("insecure");
            }
        }
        if( value == null ) {
            value = System.getProperty("insecure");
        }
        return (value != null && value.equalsIgnoreCase("true"));
    }

    public boolean isRackspace() {
        return getCloudProvider().equals(OpenStackProvider.RACKSPACE);
    }

    static private OpenStackProvider provider;

    public @Nonnull OpenStackProvider getCloudProvider() {
        provider = OpenStackProvider.getProvider(getProviderName());
        return provider;
    }

    public boolean isPostCactus() throws CloudException, InternalException {
        return (getMajorVersion() > 1 || getMinorVersion() > 0);
    }
    
    static public long parseTimestamp(String time) throws InternalException {
        if( time == null ) {
            return 0L;
        }
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            
        if( time.length() > 0 ) {
            try {
                return fmt.parse(time).getTime();
            } 
            catch( ParseException e ) {
                fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                //2012-06-18T14:47:02
                try {
                    return fmt.parse(time).getTime();
                }
                catch( ParseException encore ) {
                    fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    try {
                        return fmt.parse(time).getTime();
                    }
                    catch( ParseException again ) {
                        try {
                            return fmt.parse(time).getTime();
                        }
                        catch( ParseException whynot ) {
                            //2012-06-16 19:41:29
                            fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            try {
                                return fmt.parse(time).getTime();
                            }
                            catch( ParseException because ) {
                                throw new InternalException("Could not parse date: " + time);
                            }
                        }
                    }
                }
            }
        }
        return 0L;
    }
    
    @Override
    public @Nullable String testContext() {
        APITrace.begin(this, "Cloud.testContext");
        try {
            try {
                AuthenticationContext ctx = getAuthenticationContext();

                return (ctx == null ? null : ctx.getTenantId());
            }
            catch( Throwable t ) {
                logger.warn("Failed to test OpenStack connection context: " + t.getMessage());
                return null;
            }
        }
        finally {
            APITrace.end();
        }
    }
    
    public void createTags( String service, String resource, String resourceId, Tag... keyValuePairs ) throws CloudException, InternalException {
    	APITrace.begin(this, "Cloud.createTags");
    	try {
    		try {
    			NovaMethod method = new NovaMethod(this);
    			HashMap<String,Object> json = new HashMap<String, Object>();
    			Map<String, Object> newMeta = new HashMap<String, Object>();
    			for (int i = 0; i < keyValuePairs.length; i++) {
    				newMeta.put( keyValuePairs[i].getKey().toLowerCase(), keyValuePairs[i].getValue() != null ? keyValuePairs[i].getValue() : "");
    			}
    			json.put("metadata", newMeta);
    			method.putString(service, resource, resourceId, new JSONObject(json), "metadata");
    		} catch( Exception e ) {
    			logger.error("Error while creating tags for " + resource + " - " + resourceId + ".", e);
    		}
    	} finally {
    		APITrace.end();
    	}
    }
    
    public void updateTags( String service, String resource, String resourceId, Tag... keyValuePairs ) throws CloudException, InternalException {
    	APITrace.begin(this, "Cloud.updateTags");
    	try {
    		try {
    			NovaMethod method = new NovaMethod(this);
    			HashMap<String,Object> json = new HashMap<String, Object>();
    			Map<String, Object> newMeta = new HashMap<String, Object>();
    			for (int i = 0; i < keyValuePairs.length; i++) {
    				newMeta.put( keyValuePairs[i].getKey().toLowerCase(), keyValuePairs[i].getValue() != null ? keyValuePairs[i].getValue() : "");
    			}
    			json.put("metadata", newMeta);
    			method.postString(service, resource, resourceId, "metadata", new JSONObject(json));
    		} catch( Exception e ) {
    			logger.error("Error while updating tags for " + resource + " - " + resourceId + ".", e);
    		}
    	} finally {
    		APITrace.end();
    	}
    }
    
    public void removeTags( String service, String resource, String resourceId, Tag... keyValuePairs ) throws CloudException, InternalException {
    	APITrace.begin(this, "Cloud.removeTags");
    	try {
    		try {
    			NovaMethod method = new NovaMethod(this);
    			for (int i = 0; i < keyValuePairs.length; i++) {
    				method.deleteResource(service, resource + "/" + resourceId + "/metadata", keyValuePairs[i].getKey().toLowerCase(), null);
    			}
    		} catch( Exception e ) {
    			logger.error("Error while removing tags from " + resource + " - " + resourceId + ".", e);
    		}
    	} finally {
    		APITrace.end();
    	}
    }
}
