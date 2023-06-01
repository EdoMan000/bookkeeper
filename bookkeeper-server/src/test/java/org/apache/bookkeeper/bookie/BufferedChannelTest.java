package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
public class BufferedChannelTest {
    private BufferedChannel bufferedChannel;
    /**
     * UnpooledByteBufAllocator(boolean preferDirect):
     * This constructor allows specifying whether the allocator should prefer allocating
     * direct buffers (preferDirect set to true)
     * or heap buffers (preferDirect set to false).
     * Direct buffers are allocated outside the JVM heap,
     * which can be beneficial for scenarios involving I/O operations.
     */
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    private FileChannel fc;
    private int capacity;
    private ByteBuf src;

    @Test
    public void write() throws IOException {
        this.bufferedChannel = new BufferedChannel(this.allocator, this.fc, this.capacity);
        this.bufferedChannel.write(this.src);

    }

    @Test
    public void read() {
    }

    @Parameterized.Parameters
    public static Collection<WriteInputTuple> getWriteInputTuples(){
        List<WriteInputTuple> writeInputTupleList = new ArrayList<>();
        writeInputTupleList.add(new WriteInputTuple());
        return writeInputTupleList;
    }

    private static class WriteInputTuple {
        protected WriteInputTuple(){

        }
    }
}