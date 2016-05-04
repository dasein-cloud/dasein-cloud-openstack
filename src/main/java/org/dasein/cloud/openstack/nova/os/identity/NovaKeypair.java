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

package org.dasein.cloud.openstack.nova.os.identity;

import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.identity.AbstractShellKeySupport;
import org.dasein.cloud.identity.SSHKeypair;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.identity.ShellKeyCapabilities;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * OpenStack Nova SSH keypairs
 * @author George Reese (george.reese@enstratius.com)
 * @since 2011.10
 * @version 2011.10
 * @version 2012.04.1 Added some intelligence around features Rackspace does not support
 */
public class NovaKeypair extends AbstractShellKeySupport<NovaOpenStack> {
    static private final Logger logger = NovaOpenStack.getLogger(NovaKeypair.class, "std");

    private transient volatile NovaKeypairCapabilities capabilities;

    NovaKeypair(@Nonnull NovaOpenStack cloud) { super(cloud); }

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return getContext().getAccountNumber();
    }

    @Override
    public @Nonnull SSHKeypair createKeypair(@Nonnull String name) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.createKeypair");
        try {
            Map<String,Object> wrapper = new HashMap<>();
            Map<String,Object> json = new HashMap<>();
            NovaMethod method = new NovaMethod(getProvider());

            json.put("name", name);
            wrapper.put("keypair", json);
            JSONObject result = method.postServers("/os-keypairs", null, new JSONObject(wrapper), false);

            if( result != null && result.has("keypair") ) {
                try {
                    JSONObject ob = result.getJSONObject("keypair");

                    SSHKeypair kp = toKeypair(ob);
                    
                    if( kp == null ) {
                        throw new CommunicationException("No matching keypair was generated from " + ob.toString());
                    }
                    return kp;
                }
                catch( JSONException e ) {
                    logger.error("createKeypair(): Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Invalid response", e);
                }
            }
            logger.error("createKeypair(): No keypair was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No keypair was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void deleteKeypair(@Nonnull String keypairId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.deleteKeypair");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteServers("/os-keypairs", keypairId);
                    return;
                }
                catch( NovaException e ) {
                    if( e.getHttpCode() != HttpStatus.SC_CONFLICT ) {
                        throw e;
                    }
                }
                try { Thread.sleep(CalendarWrapper.MINUTE); }
                catch( InterruptedException e ) { /* ignore */ }
            } while( System.currentTimeMillis() < timeout );
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable String getFingerprint(@Nonnull String keypairId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.getFingerprint");
        try {
            SSHKeypair kp = getKeypair(keypairId);
            
            return (kp == null ? null : kp.getFingerprint());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable SSHKeypair getKeypair(@Nonnull String keypairId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.getKeypair");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            JSONObject ob = method.getServers("/os-keypairs", null, false);

            try {
                if( ob != null && ob.has("keypairs") ) {
                    JSONArray list = ob.getJSONArray("keypairs");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject json = list.getJSONObject(i);

                        try {
                            if( json.has("keypair") ) {
                                JSONObject kp = json.getJSONObject("keypair");

                                SSHKeypair k = toKeypair(kp);
                                
                                if( k != null && keypairId.equals(k.getProviderKeypairId()) ) {
                                    return k;
                                }
                            }
                        }
                        catch( JSONException e ) {
                            logger.error("Invalid JSON from cloud: " + e.getMessage());
                            throw new CommunicationException("Invalid JSON from cloud: " + e.getMessage(), e);
                        }
                    }
                }
                return null;
            }
            catch( JSONException e ) {
                logger.error("list(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for keypair in " + ob.toString(), e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    @Deprecated
    public @Nonnull String getProviderTermForKeypair(@Nonnull Locale locale) {
        return "keypair";
    }

    @Override
    public @Nonnull ShellKeyCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new NovaKeypairCapabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    public @Nonnull SSHKeypair importKeypair(@Nonnull String name, @Nonnull String publicKey) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.importKeypair");
        try {
            Map<String,Object> wrapper = new HashMap<>();
            Map<String,Object> json = new HashMap<>();
            NovaMethod method = new NovaMethod(getProvider());

            json.put("name", name);
            json.put("public_key", publicKey);
            wrapper.put("keypair", json);
            JSONObject result = method.postServers("/os-keypairs", null, new JSONObject(wrapper), false);

            if( result != null && result.has("keypair") ) {
                try {
                    JSONObject ob = result.getJSONObject("keypair");

                    SSHKeypair kp = toKeypair(ob);

                    if( kp == null ) {
                        throw new CommunicationException("No matching keypair was generated from " + ob.toString());
                    }
                    return kp;
                }
                catch( JSONException e ) {
                    logger.error("importKeypair(): Unable to understand create response: " + e.getMessage());
                    throw new CommunicationException("Invalid response", e);
                }
            }
            logger.error("importKeypair(): No keypair was created by the create attempt, and no error was returned");
            throw new GeneralCloudException("No keypair was created", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    private boolean verifySupport() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.verifySupport");
        try {
            NovaMethod method = new NovaMethod(getProvider());

            try {
                method.getServers("/os-keypairs", null, false);
                return true;
            }
            catch( CloudException e ) {
                if( e.getHttpCode() == 404 ) {
                    return false;
                }
                throw e;
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.isSubscribed");
        try {
            return (getProvider().getComputeServices().getVirtualMachineSupport().isSubscribed() && verifySupport());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<SSHKeypair> list() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Keypair.list");
        try {
            NovaMethod method = new NovaMethod(getProvider());
            JSONObject ob = method.getServers("/os-keypairs", null, false);
            List<SSHKeypair> keypairs = new ArrayList<>();

            try {
                if( ob != null && ob.has("keypairs") ) {
                    JSONArray list = ob.getJSONArray("keypairs");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject json = list.getJSONObject(i);

                        try {
                            if( json.has("keypair") ) {
                                JSONObject kp = json.getJSONObject("keypair");

                                keypairs.add(toKeypair(kp));
                            }
                        }
                        catch( JSONException e ) {
                            logger.error("Invalid JSON from cloud: " + e.getMessage());
                            throw new CommunicationException("Invalid JSON from cloud: " + e.getMessage(), e);
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("list(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CommunicationException("Missing JSON element for keypair in " + ob.toString(), e);
            }
            return keypairs;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    protected  @Nullable SSHKeypair toKeypair(@Nullable JSONObject json) throws InternalException, CloudException {
        if( json == null ) {
            return null;
        }
        try {
            SSHKeypair kp = new SSHKeypair();
            String name = null;

            if( json.has("private_key") ) {
                kp.setPrivateKey(json.getString("private_key").getBytes("utf-8"));
            }
            if( json.has("public_key") ) {
                kp.setPublicKey(json.getString("public_key"));
            }
            if( json.has("fingerprint") ) {
                kp.setFingerprint(json.getString("fingerprint"));
            }
            if( json.has("name") ) {
                name = json.getString("name");
            }
            if( name == null ) {
                return null;
            }
            kp.setName(name);
            kp.setProviderKeypairId(name);
            kp.setProviderOwnerId(getTenantId());
            String regionId = getContext().getRegionId();
            kp.setProviderRegionId(regionId == null ? "" : regionId);
            return kp;
        }
        catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
        catch( JSONException e ) {
            throw new InternalException(e);
        }
    }
}
