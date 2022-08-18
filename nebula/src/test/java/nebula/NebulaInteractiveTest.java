package nebula;

import org.ldbcouncil.snb.driver.workloads.interactive.*;
import org.ldbcouncil.snb.impls.workloads.interactive.InteractiveTest;
import org.ldbcouncil.snb.impls.workloads.nebula.interactive.NebulaInteractiveDb;
import org.ldbcouncil.snb.impls.workloads.nebula.converter.NebulaConverter;
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
        properties.put("spaceName", "sf300");
        properties.put("queryDir", "queries");
        properties.put("printQueryNames", "true");
        properties.put("printQueryStrings", "true");
        properties.put("printQueryResults", "true");
        properties.put("idPrefixSize", "2");
        properties.put("idPrefix","comment:c-,forum: f-,organisation:o-,person: p-,place: l-,post: s-,tag: t-,tagclass: g-");


        return properties;
    }

    @Test
    public void testConvertTime() throws ParseException {
        System.out.println(NebulaConverter.convertDateTimesToEpoch("2012-01-01T11:16:57.465000"));
    }

    @Test
    public void testShortQuery2_2() throws Exception {
        run(db, new LdbcShortQuery2PersonPosts(933L, LIMIT));
    }

    @Test
    public void testShortQuery4_2() throws Exception {
        run(db, new LdbcShortQuery4MessageContent(140738150173765L));
    }

    @Test
    public void testShortQuery4_3() throws Exception {
        run(db, new LdbcShortQuery4MessageContent(140737931644863L));
    }

    @Test
    public void testShortQuery5_2() throws Exception {
        run(db, new LdbcShortQuery5MessageCreator(140738150173765L));
    }

    @Test
    public void testShortQuery6_2() throws Exception {
        run(db, new LdbcShortQuery6MessageForum(140738150173765L));
    }

    @Test
    public void testShortQuery7_2() throws Exception {
        run(db, new LdbcShortQuery7MessageReplies(140738150173765L));
    }

    @Test
    public void testShortQuery5_3() throws Exception {
        run(db, new LdbcShortQuery5MessageCreator(140737661727089L));
    }
}
