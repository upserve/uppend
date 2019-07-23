package com.upserve.uppend.blobs;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FilePageTest {

    @Mock
    FileChannel channel;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    FilePage filePage;

    @Before
    public void before() {
      filePage = new FilePage(channel, 0, 1024);
    }

    @Test
    public void getIOExceptionTest() throws IOException {
        when(channel.read(any(ByteBuffer.class), anyLong())).thenThrow(IOException.class);
        thrown.expect(UncheckedIOException.class);
        filePage.get(0,new byte[100], 0);
    }
    @Test
    public void putIOExceptionTest() throws IOException {
        when(channel.write(any(ByteBuffer.class), anyLong())).thenThrow(IOException.class);
        thrown.expect(UncheckedIOException.class);
        filePage.put(0,new byte[100], 0);
    }
    @Test
    public void getIllegalStateText() throws IOException{
        when(channel.read(any(ByteBuffer.class), anyLong())).thenReturn(127);
        thrown.expect(IllegalStateException.class);
        filePage.get(0,new byte[130], 7);

    }
    @Test
    public void putIllegalStateText() throws IOException{
        when(channel.write(any(ByteBuffer.class), anyLong())).thenReturn(127);
        thrown.expect(IllegalStateException.class);
        filePage.put(0,new byte[130], 7);
    }
    @Test
    public void getTest() throws IOException{
        when(channel.read(any(ByteBuffer.class), anyLong())).thenReturn(123);
        filePage.get(0,new byte[130], 7);
    }
    @Test
    public void putTest() throws IOException{
        when(channel.write(any(ByteBuffer.class), anyLong())).thenReturn(123);;
        filePage.put(0,new byte[130], 7);
    }
}
