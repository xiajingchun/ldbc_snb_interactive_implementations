package nebula;

import com.ldbc.impls.workloads.ldbc.snb.interactive.InteractiveTest;
import com.ldbc.impls.workloads.ldbc.snb.nebula.interactive.NebulaInteractiveDb;
import com.ldbc.impls.workloads.ldbc.snb.nebula.converter.NebulaConverter;
import org.junit.Test;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class NebulaInteractiveTest  extends InteractiveTest {

    public NebulaInteractiveTest() {
        super(new NebulaInteractiveDb());
    }

    @Override
    protected Map<String, String> getProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("endpoint", "192.168.15.3:9669,192.168.15.5:9669,192.168.15.6:9669");
        properties.put("user", "root");
        properties.put("password", "nebula");
        properties.put("spaceName", "sf300_033");
        properties.put("queryDir", "queries");
        properties.put("printQueryNames", "true");
        properties.put("printQueryStrings", "true");
        properties.put("printQueryResults", "true");


        return properties;
    }

    @Test
    public void testConvertTime() throws ParseException {
        System.out.println(NebulaConverter.convertDateTimesToEpoch("2010-02-14T15:32:10.447"));
    }
}
