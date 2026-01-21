package com.github.shyiko.mysql.binlog.io;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ByteArrayInputStreamTest {
    @Test
    public void testReadToArray() throws Exception {
        byte[] buff = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        ByteArrayInputStream in = new ByteArrayInputStream(buff);
        assertEquals(in.getPosition(), 0);

        byte[] b = new byte[20];

        int read = in.read(b, 0, 0);
        assertEquals(read, 0);
        assertEquals(in.getPosition(), 0);

        read = in.read(b, 0, 4);
        assertEquals(read, 4);
        assertEquals(b[3], 3);
        assertEquals(in.getPosition(), 4);

        read = in.read(b, 4, 4);
        assertEquals(read, 4);
        assertEquals(b[7], 7);
        assertEquals(in.getPosition(), 8);

        read = in.read(b, 8, 4);
        assertEquals(read, 4);
        assertEquals(b[11], 11);
        assertEquals(in.getPosition(), 12);

        read = in.read(b, 12, 4);
        assertEquals(read, 4);
        assertEquals(b[15], 15);
        assertEquals(in.getPosition(), 16);

        read = in.read(b, 16, 4);
        assertEquals(read, -1);
        assertEquals(in.getPosition(), 16);
    }

    @Test
    public void testReadToArrayWithinBlockBoundaries() throws Exception {
        byte[] buff = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
        ByteArrayInputStream in = new ByteArrayInputStream(buff);
        byte[] b = new byte[8];

        in.enterBlock(4);

        int read = in.read(b, 0, 3);
        assertEquals(read, 3);
        assertEquals(b[2], 2);

        read = in.read(b, 3, 3);
        assertEquals(read, 1);
        assertEquals(b[3], 3);

        read = in.read(b, 4, 3);
        assertEquals(read, -1);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testReadToArrayWithNullBuff() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[]{});
        in.read(null, 0, 4);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadToArrayWhenLenExceedsBuffSize() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[]{0, 1, 2});
        byte[] b = new byte[1];
        in.read(b, 0, 4);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadToArrayWhenOffsetNegative() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[]{0, 1, 2});
        byte[] b = new byte[1];
        in.read(b, -1, 1);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testReadToArrayWhenLengthNegative() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[]{0, 1, 2});
        byte[] b = new byte[1];
        in.read(b, 0, -1);
    }

    @Test
    public void testPeekAndReadToArray() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[]{5, 6, 7});
        byte[] b = new byte[3];
        assertEquals(in.peek(), 5);
        int read = in.read(b, 0, 3);
        assertEquals(read, 3);
        assertEquals(b[0], 5);
        assertEquals(b[2], 7);
    }

    @Test
    public void testMarkAndReset() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[] {1, 2, 3, 4});
        in.enterBlock(4);
        in.mark(4);
        in.readInteger(4);
        in.reset();

        assertEquals(1, in.read());
        assertEquals(2, in.read());
        assertEquals(3, in.read());
        assertEquals(4, in.read());
    }
}
