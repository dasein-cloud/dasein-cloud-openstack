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

package org.dasein.cloud.openstack.nova.os.ext.rackspace.dns;

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.DNSRecord;
import org.dasein.cloud.network.DNSRecordType;
import org.dasein.cloud.network.DNSSupport;
import org.dasein.cloud.network.DNSZone;
import org.dasein.cloud.openstack.nova.os.NovaException;
import org.dasein.cloud.openstack.nova.os.NovaMethod;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.util.APITrace;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Implements Rackspace DNS services as an extension to an OpenStack cloud.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.04.1
 * @version 2012.04.1
 * @version 2013.02 updated to 2013.02 model
 */
public class RackspaceCloudDNS implements DNSSupport {
    static private final Logger logger = NovaOpenStack.getLogger(RackspaceCloudDNS.class, "std");

    static private final String RESOURCE = "/domains";
    static private final String SERVICE = "rax:dns";
    
    private NovaOpenStack provider;
    
    public RackspaceCloudDNS(NovaOpenStack provider) { this.provider = provider; }

    private @Nonnull String getTenantId() throws CloudException, InternalException {
        return provider.getContext().getAccountNumber();
    }

    @Override
    public @Nonnull DNSRecord addDnsRecord(@Nonnull String providerDnsZoneId, @Nonnull DNSRecordType recordType, @Nonnull String name, @Nonnegative int ttl, @Nonnull String... values) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.addDnsRecord");
        try {
            DNSZone zone = getDnsZone(providerDnsZoneId);
            
            if( zone == null ) {
                throw new InternalException("No such zone: " + providerDnsZoneId);
            }
            if( recordType.equals(DNSRecordType.A) || recordType.equals(DNSRecordType.AAAA) || recordType.equals(DNSRecordType.CNAME) || recordType.equals(DNSRecordType.MX) ) {
                if( name.endsWith(zone.getDomainName() + ".") ) {
                    name = name.substring(0, name.length()-1);
                }
                else if( !name.endsWith(zone.getDomainName()) ) {
                    name = name + "." + zone.getDomainName();
                }
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            DNSRecord lastRecord = null;
            
            for( String value : values ) {
                if( value != null ) {
                    NovaMethod method = new NovaMethod(provider);
        
                    HashMap<String,Object> wrapper = new HashMap<String, Object>();
                    ArrayList<Map<String,Object>> records = new ArrayList<Map<String, Object>>();
        
                    HashMap<String,Object> record = new HashMap<String, Object>();
        
                    record.put("name", name);
                    record.put("data", value);
                    record.put("type", recordType.name());
                    record.put("ttl", ttl > 0 ? ttl : 3600);
                    
                    records.add(record);
        
                    wrapper.put("records", records);
        
                    JSONObject response = method.postString(SERVICE, RESOURCE, providerDnsZoneId + "/records", new JSONObject(wrapper), false);
        
                    try {
                        if( response != null && response.has("jobId") ) {
                            response = waitForJob(response.getString("jobId"));
                            if( response != null && response.has("records") ) {
                                JSONArray list = response.getJSONArray("records");

                                for( int i=0; i<list.length(); i++ ) {
                                    DNSRecord r = toRecord(ctx, zone, list.getJSONObject(i));

                                    if( r != null ) {
                                        lastRecord = r;
                                    }
                                }
                            }
                        }
                    }
                    catch( JSONException e ) {
                        logger.error("createDnsZone(): JSON error parsing response: " + e.getMessage());
                        throw new CommunicationException("Unable to understand parsing response: " + e.getMessage(), e);
                    }
                }
            }
            if( lastRecord == null ) {
                logger.error("addDnsRecord(): No record was created, but no error specified");
                throw new GeneralCloudException("No record was created, but no error specified", CloudErrorType.GENERAL);
            }
            return lastRecord;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String createDnsZone(@Nonnull String domainName, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.createDnsZone");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);

            HashMap<String,Object> wrapper = new HashMap<String, Object>();
            ArrayList<Map<String,Object>> domains = new ArrayList<Map<String, Object>>();
            
            HashMap<String,Object> domain = new HashMap<String, Object>();
            
            domain.put("name", domainName);
            domain.put("comment", description);
            domain.put("emailAddress", "postmaster@" + domainName);

            domains.add(domain);

            wrapper.put("domains", domains);

            JSONObject response = method.postString(SERVICE, RESOURCE, null, new JSONObject(wrapper), false);

            try {
                if( response != null && response.has("jobId") ) {
                    response = waitForJob(response.getString("jobId"));
                    if( response != null && response.has("domains") ) {
                        JSONArray list = response.getJSONArray("domains");

                        for( int i=0; i<list.length(); i++ ) {
                            DNSZone zone = toZone(ctx, list.getJSONObject(i));

                            if( zone != null ) {
                                return zone.getProviderDnsZoneId();
                            }
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("createDnsZone(): JSON error parsing response: " + e.getMessage());
                throw new CommunicationException("Unable to understand createDnsZone() response: " + e.getMessage(), e);
            }
            logger.error("createDnsZone(): No zone was created, but no error specified");
            throw new GeneralCloudException("No zone was created, but no error specified", CloudErrorType.GENERAL);

        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull List<String> lookupRecord(DNSRecord record) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.lookupRecord");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject response = method.getResource(SERVICE, RESOURCE, record.getProviderZoneId() + "/records", false);

            if( response == null ) {
                return null;
            }
            ArrayList<String> ids = new ArrayList<String>();
            
            try {
                if( response.has("records") ) {
                    JSONArray list = response.getJSONArray("records");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject item = list.getJSONObject(i);

                        if( item != null ) {
                            String n = (item.has("name") ? item.getString("name") : null);
                            String id = (item.has("id") ? item.getString("id") : null);
                            
                            if( n == null || id == null ) {
                                continue;
                            }
                            if( record.getName().equals(n) || record.getName().equals(n + ".") ) {
                                ids.add(id);
                            }
                        }
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("lookupRecord(): JSON error parsing response: " + e.getMessage());
                throw new GeneralCloudException("createDnsZone(): JSON error parsing response", CloudErrorType.GENERAL);
            }
            return ids;
        }
        finally {
            APITrace.end();
        }
    }
    
    @Override
    public void deleteDnsRecords(@Nonnull DNSRecord... dnsRecords) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.deleteDnsRecords");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            for( DNSRecord record : dnsRecords ) {
                NovaMethod method = new NovaMethod(provider);

                List<String> ids = lookupRecord(record);
                
                for( String id : ids ) {
                    method.deleteResource(SERVICE, RESOURCE, record.getProviderZoneId() + "/records/" + id, null);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void deleteDnsZone(@Nonnull String providerDnsZoneId) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.deleteDnsZone");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);

            method.deleteResource(SERVICE, RESOURCE, providerDnsZoneId, null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public DNSZone getDnsZone(@Nonnull String providerDnsZoneId) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.getDnsZone");
        try {
            CompleteDNS dns = getCompleteDNS(providerDnsZoneId, false);

            return (dns == null ? null : dns.domain);
        }
        finally {
            APITrace.end();
        }
    }

    private void listSubdomains(@Nonnull ProviderContext ctx, @Nonnull List<DNSZone> intoList, @Nonnull DNSZone parent, @Nonnull JSONArray subdomains) throws CloudException, InternalException {
        try {
            for( int i=0; i<subdomains.length(); i++ ) {
                DNSZone z = toZone(ctx, subdomains.getJSONObject(i));
                
                if( z != null ) {
                    z.setNameservers(parent.getNameservers());
                    intoList.add(z);
                }
            }
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to understand response: " + e.getMessage(), e);
        }
    }
    
    static private class CompleteDNS {
        public DNSZone domain;
        public List<DNSZone> subdomains;
    }
    
    private CompleteDNS getCompleteDNS(@Nonnull String providerDnsZoneId, boolean withSubdomains) throws CloudException, InternalException{
        Logger std = NovaOpenStack.getLogger(RackspaceCloudDNS.class, "std");

        if( std.isTraceEnabled() ) {
            std.trace("ENTER: " + RackspaceCloudDNS.class.getName() + ".getCompleteDNS()");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                std.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            String query = providerDnsZoneId + "?showRecords=true";

            if( withSubdomains ) {
                query = query + "&showSubdomains=true";
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject response = method.getResource(SERVICE, RESOURCE, query, false);

            if( response == null ) {
                return null;
            }
            try {
                DNSZone zone = toZone(ctx, response);

                if( zone != null ) {
                    CompleteDNS dns = new CompleteDNS();

                    dns.domain = zone;
                    dns.subdomains = new ArrayList<DNSZone>();
                            
                    JSONObject subdomains = (response.has("subdomains") ? response.getJSONObject("subdomains") : null);
                            
                    if( subdomains != null ) {
                        JSONArray domains = (subdomains.has("domains") ? subdomains.getJSONArray("domains") : null);
                                
                        if( domains != null ) {
                            listSubdomains(ctx, dns.subdomains, zone, domains);
                        }
                    }
                    return dns;
                }
            }
            catch( JSONException e ) {
                std.error("getCompleteDNS(): JSON error parsing response: " + e.getMessage());
                throw new CommunicationException("Unable to understand getCompleteDNS() response: " + e.getMessage(), e);
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + RackspaceCloudDNS.class.getName() + ".getCompleteDNS()");
            }
        }
    }

    @Override
    public @Nonnull String getProviderTermForRecord(@Nonnull Locale locale) {
        return "record";
    }

    @Override
    public @Nonnull String getProviderTermForZone(@Nonnull Locale locale) {
        return "domain";
    }

    @Override
    public @Nonnull Iterable<DNSRecord> listDnsRecords(@Nonnull String providerDnsZoneId, @Nullable DNSRecordType forType, @Nullable String name) throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.listDnsRecords");
        try {
            DNSZone zone = getDnsZone(providerDnsZoneId);
            
            if( zone == null ) {
                throw new InternalException("No such zone: " + providerDnsZoneId);
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject response = method.getResource(SERVICE, RESOURCE, providerDnsZoneId + "/records", false);

            if( response == null ) {
                return Collections.emptyList();
            }
            ArrayList<DNSRecord> records = new ArrayList<DNSRecord>();
            try {
                int count = 0, total = 0;

                if( response.has("totalEntries") ) {
                    total = response.getInt("totalEntries");
                }
                while( response != null ) {
                    int current = 0;

                    if( response.has("records") ) {
                        JSONArray list = response.getJSONArray("records");

                        current = list.length();
                        count += current;
                        for( int i=0; i<list.length(); i++ ) {
                            DNSRecord record = toRecord(ctx, zone, list.getJSONObject(i));
    
                            if( record != null ) {
                                if( forType == null || forType.equals(record.getType()) ) {
                                    if( name == null || name.equals(record.getName()) ) {
                                        records.add(record);
                                    }
                                }
                            }
                        }
                    }
                    response = null;
                    if( current > 0 && count < total ) {
                        response = method.getResource(SERVICE, RESOURCE, providerDnsZoneId + "/records?offset=" + count, false);
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("listDnsRecords(): JSON error parsing response: " + e.getMessage());
                throw new CommunicationException("Unable to understand listDnsRecords() response: " + e.getMessage(), e);
            }
            return records;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listDnsZoneStatus() throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.listDnsZoneStatus");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject response = method.getResource(SERVICE, RESOURCE, null, false);

            if( response == null ) {
                return Collections.emptyList();
            }
            ArrayList<ResourceStatus> zones = new ArrayList<ResourceStatus>();

            try {
                int count = 0, total = 0;

                if( response.has("totalEntries") ) {
                    total = response.getInt("totalEntries");
                }
                while( response != null ) {
                    int current = 0;

                    if( response.has("domains") ) {
                        JSONArray list = response.getJSONArray("domains");

                        current = list.length();
                        count += current;
                        for( int i=0; i<list.length(); i++ ) {
                            JSONObject item = list.getJSONObject(i);

                            if( item != null && item.has("id") ) {
                                zones.add(new ResourceStatus(item.getString("id"), true));
                            }
                        }
                    }
                    response = null;
                    if( current > 0 && count < total ) {
                        response = method.getResource(SERVICE, RESOURCE, "?offset=" + count, false);
                    }
                }
            }
            catch( JSONException e ) {
                throw new CommunicationException("Invalid response: " + e.getMessage(), e);
            }
            return zones;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<DNSZone> listDnsZones() throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.listDnsZones");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                logger.error("No context exists for this request");
                throw new InternalException("No context exists for this request");
            }
            NovaMethod method = new NovaMethod(provider);
            JSONObject response = method.getResource(SERVICE, RESOURCE, null, false);
            
            if( response == null ) {
                return Collections.emptyList();
            }
            ArrayList<DNSZone> zones = new ArrayList<DNSZone>();

            try {
                int count = 0, total = 0;

                if( response.has("totalEntries") ) {
                    total = response.getInt("totalEntries");
                }
                while( response != null ) {
                    int current = 0;

                    if( response.has("domains") ) {
                        JSONArray list = response.getJSONArray("domains");

                        current = list.length();
                        count += current;
                        for( int i=0; i<list.length(); i++ ) {
                            JSONObject item = list.getJSONObject(i);

                            if( item != null && item.has("id") ) {
                                CompleteDNS dns = getCompleteDNS(item.getString("id"), true);

                                if( dns != null ) {
                                    zones.add(dns.domain);
                                    zones.addAll(dns.subdomains);
                                }
                            }
                        }
                    }
                    response = null;
                    if( current > 0 && count < total ) {
                        response = method.getResource(SERVICE, RESOURCE, "?offset=" + count, false);
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("listDnsZones(): JSON error parsing response: " + e.getMessage());
                throw new CommunicationException("Unable to understand listDnsZones() response: " + e.getMessage(), e);
            }
            return zones;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(provider, "DNS.isSubscribed");
        try {
            return (provider.getCloudName().contains("Rackspace") && provider.getAuthenticationContext().getServiceUrl(SERVICE) != null);
        }
        finally {
            APITrace.end();
        }

    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }
    
    private @Nullable DNSRecord toRecord(@SuppressWarnings("UnusedParameters") @Nonnull ProviderContext ctx, @Nonnull DNSZone zone, @Nullable JSONObject json)  throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        try {
            String recordId = (json.has("id") ? json.getString("id") : null);
            
            if( recordId == null ) {
                return null;
            }
            String name = (json.has("name") ? json.getString("name") : null);
            
            if( name == null ) {
                return null;
            }
            if( name.endsWith(zone.getDomainName()) ) {
                name = name + ".";
            }
            DNSRecordType recordType = DNSRecordType.A;
            String type = (json.has("type") ? json.getString("type") : null);
            
            if( type != null ) {
                recordType = DNSRecordType.valueOf(type.toUpperCase());
            }
            String data = (json.has("data") ? json.getString("data") : null);
            int ttl = (json.has("ttl") ? json.getInt("ttl") : 3600);
            
            DNSRecord record = new DNSRecord();
            
            record.setName(name);
            record.setProviderZoneId(zone.getProviderDnsZoneId());
            record.setTtl(ttl);
            record.setType(recordType);
            record.setValues(data == null ? new String[0] : new String[] { data });

            return record;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to understand response: " + e.getMessage(), e);
        }        
    }

    private @Nullable DNSZone toZone(@Nonnull ProviderContext ctx, @Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        try {
            String zoneId = (json.has("id") ? json.getString("id") : null);
            
            if( zoneId == null ) {
                return null;
            }
            
            String name = (json.has("name") ? json.getString("name") : null);
            
            if( name == null ) {
                return null;
            }
            
            String description = (json.has("comment") ? json.getString("comment") : null);
            
            if( description == null ) {
                description = name;
            }
            JSONArray nameservers = (json.has("nameservers") ? json.getJSONArray("nameservers") : null);
            
            DNSZone zone = new DNSZone();

            zone.setDescription(description);
            zone.setDomainName(name);
            zone.setName(name);
            zone.setProviderDnsZoneId(zoneId);
            zone.setProviderOwnerId(getTenantId());
            if( nameservers != null ) {
                String[] ns = new String[nameservers.length()];
                
                for( int i=0; i<nameservers.length(); i++ ) {
                    JSONObject ob = nameservers.getJSONObject(i);
                    
                    if( ob.has("name") ) {
                        ns[i] = ob.getString("name");
                    }
                }
                zone.setNameservers(ns);
            }
            else {
                zone.setNameservers(new String[0]);
            }
            return zone;
        }
        catch( JSONException e ) {
            throw new CommunicationException("Unable to understand  response: " + e.getMessage(), e);
        }
    }
    
    private JSONObject waitForJob(String jobId) throws CloudException, InternalException {
        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20);

        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context exists for this request");
        }
        while( System.currentTimeMillis() < timeout ) {
            try {
                NovaMethod method = new NovaMethod(provider);
                JSONObject response = method.getResource(SERVICE, "/status", jobId + "?showDetails=true", false);
    
                if( response == null ) {
                    throw new GeneralCloudException("Job disappeared", CloudErrorType.GENERAL);
                }
                String status = (response.has("status")? response.getString("status") : null);
                
                if( status == null ) {
                    throw new GeneralCloudException("No job status", CloudErrorType.GENERAL);
                }
                if( status.equalsIgnoreCase("completed") ) {
                    if( response.has("response") ) {
                        return response.getJSONObject("response");
                    }
                }
                else if( status.equalsIgnoreCase("error") ) {
                    if( response.has("error") ) {
                        JSONObject error = response.getJSONObject("error");
                        
                        if( error == null ) {
                            throw new InternalException("Unknown error");
                        }
                        int code = (error.has("code") ? error.getInt("code") : 418);
                        
                        throw new NovaException(NovaException.parseException(code, error.toString()));
                    }
                    throw new InternalException("Unknown error");
                }
            }
            catch( JSONException e ) {
                throw new CommunicationException("Unable to understand response: " + e.getMessage(), e);
            }
            try { Thread.sleep(CalendarWrapper.SECOND * 30); }
            catch( InterruptedException ignore ) { }
        }
        throw new GeneralCloudException("Operation timed out", CloudErrorType.GENERAL);
    }
}