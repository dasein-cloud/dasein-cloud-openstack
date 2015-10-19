package org.dasein.cloud.openstack.nova.os;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by mariapavlova on 08/10/2015.
 */
public class AuthenticationContextTest {
    private AuthenticationContext authenticationContext;
    private final static String REGION_ID = "testRegionId";
    private final static String TOKEN = "testToken";
    private final static String TENANT_ID = "testTenantId";
    private final static String TEST_URL = "testUrl";
    private final static String SERVICE_URL = "testServiceUrl";
    private final static String STORAGE_TOKEN = "testStorageToken";

    @Before
    public void before() {
        Map<String, Map<String, String>> services = new HashMap<String, Map<String, String>>();
        Map<String, String> computeMap = new HashMap<String, String>();
        computeMap.put(REGION_ID, TEST_URL);
        Map<String, String> myRegionMap = new HashMap<String, String>();
        myRegionMap.put(REGION_ID, SERVICE_URL);
        services.put("compute", computeMap);
        services.put("myService", myRegionMap);
        authenticationContext = new AuthenticationContext(REGION_ID, TOKEN, TENANT_ID, services ,STORAGE_TOKEN);

    }
    @Test
    public void getComputeUrlTest() {
    assertEquals("Returned test url is not as expected", TEST_URL, authenticationContext.getComputeUrl());
    }
    @Test
    public void getServiceUrlTest() {
        assertEquals("Returned service url is not as expected", SERVICE_URL, authenticationContext.getServiceUrl("myService"));
    }

}
