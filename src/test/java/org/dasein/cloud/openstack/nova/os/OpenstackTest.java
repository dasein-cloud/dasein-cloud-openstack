package org.dasein.cloud.openstack.nova.os;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;

/**
 * Base class for tests, contains useful common methods
 */
public class OpenStackTest {

    protected JSONObject readJson(String filename) {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(filename);
            if( is == null ) {
                throw new RuntimeException("File not found: " + filename);
            }
            String jsonText = IOUtils.toString(is);
            return new JSONObject(jsonText);
        }
        catch( JSONException e ) {
            throw new RuntimeException(e);
        }
        catch( IOException e ) {
            throw new RuntimeException(e);
        }
    }
}
