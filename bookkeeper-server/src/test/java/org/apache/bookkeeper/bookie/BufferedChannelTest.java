package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.rules.ExpectedException;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
     * Category Partitioning for fc is {notEmpty, empty, null, invalidInstance}
     */
    private FileChannel fc;
    /**
     * Category Partitioning for capacity is {<0 ,>0, =0}
     */
    private final int capacity;
    /**
     * Category Partitioning for src is {notEmpty, empty, null, invalidInstance}
     */
    private ByteBuf src;
    /**
     * Category Partitioning for srcSize is {<0 ,>0, =0} --> multidimensional {< capacity, = capacity, > capacity}
     */
    private final int srcSize;
    private byte[] data;
    private int numOfExistingBytes;

    private enum STATE_OF_OBJ {
        EMPTY,
        NOT_EMPTY,
        NULL,
        INVALID
    }
    private final STATE_OF_OBJ stateOfFc;
    private final STATE_OF_OBJ stateOfSrc;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public BufferedChannelTest(WriteInputTuple writeInputTuple) {
        this.capacity = writeInputTuple.capacity();
        this.stateOfFc = writeInputTuple.stateOfFc();
        this.stateOfSrc = writeInputTuple.stateOfSrc();
        this.srcSize = writeInputTuple.srcSize();
        this.numOfExistingBytes = 0;
        if(writeInputTuple.expectedException() != null){
            this.expectedException.expect(writeInputTuple.expectedException());
        }
    }

    /**
     * -----------------------
     * Boundary analysis:
     * -----------------------
     * capacity: -1 ; 10; 0
     * srcSize: capacity-1; capacity; capacity+1
     * src: {notEmpty_ByteBuff, empty_ByteBuff, null, invalidInstance}
     * fc: {notEmpty_FileChannel, empty_FileChannel, null, invalidInstance}
     */

    @Parameterized.Parameters
    public static Collection<WriteInputTuple> getWriteInputTuples(){
        List<WriteInputTuple> writeInputTupleList = new ArrayList<>();
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.INVALID, null));
        writeInputTupleList.add(new WriteInputTuple(0, 0, STATE_OF_OBJ.NULL, STATE_OF_OBJ.EMPTY, NullPointerException.class));
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE_OF_OBJ.NOT_EMPTY, STATE_OF_OBJ.EMPTY, null));
        writeInputTupleList.add(new WriteInputTuple(6, 0, STATE_OF_OBJ.NOT_EMPTY, STATE_OF_OBJ.EMPTY, null));
        return writeInputTupleList;
    }

    private record WriteInputTuple(int capacity,
                                   int srcSize,
                                   STATE_OF_OBJ stateOfFc,
                                   STATE_OF_OBJ stateOfSrc,
                                   Class<? extends Exception> expectedException) {
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
            if (this.stateOfFc == STATE_OF_OBJ.NOT_EMPTY || this.stateOfFc == STATE_OF_OBJ.EMPTY) {
                if(this.stateOfFc == STATE_OF_OBJ.NOT_EMPTY) {
                    try (FileOutputStream fileOutputStream = new FileOutputStream("testDir/BufChanReadTest/writeToThisFile.log")) {
                        this.numOfExistingBytes = random.nextInt(10);
                        byte[] alreadyExistingBytes = new byte[this.numOfExistingBytes];
                        random.nextBytes(alreadyExistingBytes);
                        fileOutputStream.write(alreadyExistingBytes);
                    }
                }
                this.fc = FileChannel.open(Paths.get("testDir/BufChanReadTest/writeToThisFile.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                /*
                 * fc.position(this.fc.size()) is used to set the position of the file channel (fc) to the end of the file.
                 * This operation ensures that any subsequent write operations will append data to the existing content
                 * of the file rather than overwrite it.
                 * (we did this also because StandardOpenOption.READ and .APPEND is not allowed together)
                 */
                this.fc.position(this.fc.size());
                this.data = new byte[this.srcSize];
                random.nextBytes(this.data);
            } else if (this.stateOfFc == STATE_OF_OBJ.NULL) {
                this.fc = null;
            } else if (this.stateOfFc == STATE_OF_OBJ.INVALID) {
                this.fc = getMockedInvalidFcInstance();
            }
            assignSrc();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private FileChannel getMockedInvalidFcInstance() {
        FileChannel invalidFc = mock(FileChannel.class);

        return  invalidFc;
    }

    private void assignSrc(){
        this.src = Unpooled.directBuffer();
        if(this.stateOfSrc == STATE_OF_OBJ.NOT_EMPTY) {
            this.src.writeBytes(this.data);
        } else if (this.stateOfSrc == STATE_OF_OBJ.NULL) {
            this.src = null;
        } else if (this.stateOfSrc == STATE_OF_OBJ.INVALID) {
            this.src = getMockedInvalidSrcInstance();
        }
    }

    private ByteBuf getMockedInvalidSrcInstance() {
        ByteBuf invalidByteBuf = mock(ByteBuf.class);
        when(invalidByteBuf.readableBytes()).thenReturn(-1);
        return invalidByteBuf;
    }

    @After
    public void cleanupEachTime(){
        try {
            if(this.stateOfFc != STATE_OF_OBJ.NULL) {
                this.fc.close();
            }
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
        File parentDirectory = new File("testDir");
        parentDirectory.delete();
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
             * NB]: while adding entries to BufferedChannel if src has reached its capacity
             * then it will call flush method and the data gets added to the file buffer.
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