package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

@RunWith(value = Parameterized.class)
public class BufferedChannelTest {
    /**
     * UnpooledByteBufAllocator(boolean preferDirect):
     * This constructor allows specifying whether the allocator should prefer allocating
     * direct buffers (preferDirect set to true)
     * or heap buffers (preferDirect set to false).
     * Direct buffers are allocated outside the JVM heap,
     * which can be beneficial for scenarios involving I/O operations.
     */
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    /**
     * Category Partitioning for fc is {notEmpty, empty, null}
     */
    private FileChannel fc;
    /**
     * Category Partitioning for capacity is {<0 ,>0, =0}
     */
    private int capacity;
    /**
     * Category Partitioning for src is {notEmpty, empty, null}
     */
    private ByteBuf src;
    /**
     * Category Partitioning for srcSize is {<0 ,>0, =0} --> {< capacity, = capacity, > capacity}
     */
    private int srcSize;
    private byte[] data;
    private boolean isSrcEmpty;
    private int numOfExistingBytes;

    public BufferedChannelTest(WriteInputTuple writeInputTuple) {
        this.capacity = writeInputTuple.capacity();
        this.srcSize = writeInputTuple.srcSize();
        this.isSrcEmpty = writeInputTuple.isSrcEmpty();
        this.numOfExistingBytes = 0;
    }

    @Parameterized.Parameters
    public static Collection<WriteInputTuple> getWriteInputTuples(){
        List<WriteInputTuple> writeInputTupleList = new ArrayList<>();
        writeInputTupleList.add(new WriteInputTuple(10, 8, true));
        writeInputTupleList.add(new WriteInputTuple(6, 10, true));
        return writeInputTupleList;
    }

    private static final class WriteInputTuple {
        private final int capacity;
        private final int srcSize;
        private final boolean isSrcEmpty;

        private WriteInputTuple(int capacity, int srcSize, boolean isSrcEmpty) {
            this.capacity = capacity;
            this.srcSize = srcSize;
            this.isSrcEmpty = isSrcEmpty;
        }

        public int capacity() {
            return capacity;
        }

        public int srcSize() {
            return srcSize;
        }

        public boolean isSrcEmpty() {
            return isSrcEmpty;
        }

    }

    @BeforeClass
    public static void setUpOnce(){
        File newLogFileDirs = new File("testDir/BufChanReadTest");
        if(!newLogFileDirs.exists()){
            newLogFileDirs.mkdirs();
        }

        File oldLogFile = new File("testDir/BufChanReadTest/writeToThisFile.log");
        if(oldLogFile.exists()){
            oldLogFile.delete();
        }
    }

    @Before
    public void setUpEachTime(){
        try {
            Random random = new Random();
            if (!this.isSrcEmpty) {
                try (FileOutputStream fileOutputStream = new FileOutputStream("testDir/BufChanReadTest/writeToThisFile.log")) {
                    this.numOfExistingBytes = random.nextInt(10);
                    byte[] alreadyExistingBytes = new byte[this.numOfExistingBytes];
                    random.nextBytes(alreadyExistingBytes);
                    fileOutputStream.write(alreadyExistingBytes);
                }
            }
            this.fc = FileChannel.open(Paths.get("testDir/BufChanReadTest/writeToThisFile.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            /*
              fc.position(this.fc.size()) is used to set the position
              of the file channel (fc) to the end of the file.
              This operation ensures that any subsequent write
              operations will append data to the existing content
              of the file rather than overwrite it.
            */
            this.fc.position(this.fc.size());
            this.data = new byte[this.srcSize];
            this.src = Unpooled.directBuffer();
            random.nextBytes(this.data);
            this.src.writeBytes(this.data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void cleanupEachTime(){
        try {
            this.fc.close();
            File oldLogFile = new File("testDir/BufChanReadTest/writeToThisFile.log");
            if(oldLogFile.exists()){
                oldLogFile.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void cleanupOnce(){
        File newLogFileDirs = new File("testDir/BufChanReadTest");
        deleteDirectoryRecursive(newLogFileDirs);
    }

    private static void deleteDirectoryRecursive(File directories) {
        if (directories.exists()) {
            File[] files = directories.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectoryRecursive(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directories.delete();
        }
    }

    @Test
    public void write() {
        try {
            BufferedChannel bufferedChannel = new BufferedChannel(this.allocator, this.fc, this.capacity);
            bufferedChannel.write(this.src);

            /*
             * if flush is not called then internal write buffer is not flushed,
             * but while adding entries to BufferedChannel if src has
             * reached its capacity then it will call flush method
             * and the data gets added to the file buffer.
             */
            int expectedNumOfBytesInWriteBuff = (this.srcSize < this.capacity) ? this.srcSize : this.srcSize % this.capacity ;
            int expectedNumOfBytesInFc = (this.srcSize < this.capacity) ? 0 : this.srcSize - expectedNumOfBytesInWriteBuff ;

            byte[] actualBytesInWriteBuff = new byte[expectedNumOfBytesInWriteBuff];
            bufferedChannel.writeBuffer.getBytes(0, actualBytesInWriteBuff);

            //We only take expectedNumOfBytesInWriteBuff bytes from this.data because the rest would have been flushed onto the fc
            byte[] expectedBytesInWriteBuff = Arrays.copyOfRange(this.data, this.data.length - expectedNumOfBytesInWriteBuff, this.data.length);
            Assert.assertEquals("BytesInWriteBuff Check Failed", Arrays.toString(actualBytesInWriteBuff), Arrays.toString(expectedBytesInWriteBuff));

            ByteBuffer actualBytesInFc = ByteBuffer.allocate(expectedNumOfBytesInFc);
            this.fc.position(this.numOfExistingBytes);
            this.fc.read(actualBytesInFc);
            //We take everything that has supposedly been flushed onto the fc
            byte[] expectedBytesInFc = Arrays.copyOfRange(this.data, 0, expectedNumOfBytesInFc);
            Assert.assertEquals("BytesInFc Check Failed", Arrays.toString(actualBytesInFc.array()), Arrays.toString(expectedBytesInFc));
            Assert.assertEquals("BufferedChannelPosition Check Failed", this.srcSize + this.numOfExistingBytes, bufferedChannel.position());
            Assert.assertTrue(true);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}