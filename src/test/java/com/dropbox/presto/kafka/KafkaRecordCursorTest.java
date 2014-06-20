package com.dropbox.presto.kafka;

import junit.framework.Assert;
import junit.framework.TestCase;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

public class KafkaRecordCursorTest extends TestCase {
    public KafkaRecordCursorTest()
    {

    }

    public void testBoolean()
    {
        String json = "{\"is_shared_ns\": false}";
        Assert.assertEquals(getBoolean(json, "is_shared_ns"), false);
    }

    public void testLong()
    {
        String json = "{\"user_id\":9223372036854775807}";
        Assert.assertEquals(getLong(json, "user_id"), 9223372036854775807L);
    }

    private boolean getBoolean(String str, String key)
    {
        JSONObject msg = (JSONObject)JSONValue.parse(str);
        return (Boolean) msg.get(key);
    }

    private long getLong(String str, String key)
    {
        JSONObject msg = (JSONObject)JSONValue.parse(str);
        return (Long) msg.get(key);
    }
}