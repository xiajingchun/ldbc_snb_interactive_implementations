package nebula;

import com.ldbc.impls.workloads.ldbc.snb.db.BaseDb;
import com.ldbc.impls.workloads.ldbc.snb.interactive.InteractiveTest;
import com.ldbc.impls.workloads.ldbc.snb.nebula.interactive.NebulaInteractiveDb;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class NebulaInteractiveTest  extends InteractiveTest {

    public NebulaInteractiveTest() {
        super(new NebulaInteractiveDb());
    }

    @Test
    public void testStr() throws Exception {
        String friendIdStr = "person-123";
        Assert.assertEquals("123", friendIdStr.substring(7));
        Assert.assertEquals(123, Long.parseLong(friendIdStr.substring(7)));
    }

    @Override
    protected Map<String, String> getProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("endpoint", "192.168.15.3:9669,192.168.15.5:9669,192.168.15.6:9669");
        properties.put("user", "root");
        properties.put("password", "nebula");
        properties.put("queryDir", ""); // TODO:
        properties.put("printQueryNames", "true");
        properties.put("printQueryStrings", "true");
        properties.put("printQueryResults", "true");
        return properties;
    }
}
