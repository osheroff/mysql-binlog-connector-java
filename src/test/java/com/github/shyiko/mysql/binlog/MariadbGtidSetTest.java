package com.github.shyiko.mysql.binlog;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class MariadbGtidSetTest {

    @Test
    public void testAdd() {
        MariadbGtidSet gtidSet = new MariadbGtidSet("0-102-7255");
        gtidSet.add("0-102-7256");
        gtidSet.add("0-102-7257");
        gtidSet.add("0-102-7259");
        gtidSet.add("1-102-7300");
        assertNotEquals(gtidSet.toString(), "1-102-7300");
        assertNotEquals(gtidSet.toString(), "0-102-7259");
        assertEquals(gtidSet.toString(), "0-102-7259,1-102-7300");
    }

    @Test
    public void testEmptySet() {
        assertEquals(new MariadbGtidSet("").toString(), "");
    }

    @Test
    public void testEquals() {
        assertEquals(new MariadbGtidSet(""), new MariadbGtidSet(null));
        assertEquals(new MariadbGtidSet(""), new MariadbGtidSet(""));
        assertEquals(new MariadbGtidSet("0-0-7404"), new MariadbGtidSet("0-0-7404"));
    }
}
