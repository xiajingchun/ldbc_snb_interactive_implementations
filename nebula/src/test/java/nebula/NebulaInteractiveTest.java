package nebula;

import org.junit.Assert;
import org.junit.Test;

public class NebulaInteractiveTest {

    @Test
    public void testStr() throws Exception {
        String friendIdStr = "person-123";
        Assert.assertEquals("123", friendIdStr.substring(7));
        Assert.assertEquals(123, Long.parseLong(friendIdStr.substring(7)));
    }
}
