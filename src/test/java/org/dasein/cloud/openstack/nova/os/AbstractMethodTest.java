package org.dasein.cloud.openstack.nova.os;

import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Created by mariapavlova on 08/09/2015.
 */
public class AbstractMethodTest {
    @Test
    public void postStringThrowsExceptionFor413() {
        final int expectedStatusCode = 413;
        InputStream json = getClass().getClassLoader().getResourceAsStream("nova/fixtures/error413.json");
        NovaMethod method = mock(NovaMethod.class);
        HttpClient mockClient = mock(HttpClient.class);
        HttpResponse response = mock(HttpResponse.class);
        when(response.getAllHeaders()).thenReturn(new Header[]{new Header() {
            @Override public String getName() {
                return "header-name";
            }

            @Override public String getValue() {
                return "header-value";
            }

            @Override public HeaderElement[] getElements() throws ParseException {
                return new HeaderElement[0];
            }
        }});

        when(mockClient.getConnectionManager()).thenReturn(mock(ClientConnectionManager.class));
        when(response.getStatusLine()).thenReturn(new StatusLine() {
            @Override public ProtocolVersion getProtocolVersion() {
                return new ProtocolVersion("HTTP", 1, 1);
            }

            @Override public int getStatusCode() {
                return expectedStatusCode;
            }

            @Override public String getReasonPhrase() {
                return "bogus";
            }
        });
        HttpEntity entity = mock(HttpEntity.class);
        try {
            when(entity.getContent()).thenReturn(json);
            when(entity.getContentLength()).thenReturn(( long ) json.available());
            when(response.getEntity()).thenReturn(entity);
        }
        catch( IOException e ) {
            e.printStackTrace();
        }

        try {
            when(mockClient.execute(any(HttpPost.class))).thenReturn(response);
            when(method.getClient()).thenReturn(mockClient);
            when(method.postString("bogus", "bogus", null, "bogus")).thenCallRealMethod();
            method.postString("bogus", "bogus", null, "bogus");
            assertTrue("Exception should have been thrown", false);
        }
        catch( CloudException e ) {
            assertEquals("Exception HTTP status code does not match", expectedStatusCode, e.getHttpCode());
            assertEquals("Exception provider code does not match", "Over Limit", e.getProviderCode());
            assertEquals("Exception message does not match", "VolumeLimitExceeded: Maximum number of volumes allowed (10) exceeded", e.getMessage());
            assertEquals("Exception error type does not match", CloudErrorType.CAPACITY, e.getErrorType());
        }
        catch( InternalException e ) {
            e.printStackTrace();
        }
        catch( IOException e ) {
            e.printStackTrace();
        }
    }
}
