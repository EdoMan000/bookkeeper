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
public class BufferedChannelWritingTest {
    public static final Class<? extends Exception> SUCCESS = null;
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
     * Category Partitioning for fc is:<br>
     * {notEmpty, empty, null, invalidInstance}
     */
    private FileChannel fc;
    /**
     * Category Partitioning for capacity is:<br>
     * {<=0, >0}
     */
    private final int capacity;
    /**
     * Category Partitioning for src is:<br>
     * {notEmpty, empty, null, invalidInstance}
     */
    private ByteBuf src;
    /**
     * Category Partitioning for srcSize is:<br>
     * {<0, >0 ,=0} <br>
     * turns out like --> {< capacity, = capacity, > capacity}
     */
    private final int srcSize;
    private byte[] data;
    private int numOfExistingBytes;
    private long unpersistedBytesBound;

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

    public BufferedChannelWritingTest(WriteInputTuple writeInputTuple) {
        this.capacity = writeInputTuple.capacity();
        this.stateOfFc = writeInputTuple.stateOfFc();
        this.stateOfSrc = writeInputTuple.stateOfSrc();
        this.srcSize = writeInputTuple.srcSize();
        this.unpersistedBytesBound = writeInputTuple.unpersistedBytesBound();
        this.numOfExistingBytes = 0;
        if(writeInputTuple.expectedException() != null){
            this.expectedException.expect(writeInputTuple.expectedException());
        }
    }

    /**
     * ---------------------------------------------------------------------<br>
     * Boundary analysis:<br>
     * ---------------------------------------------------------------------<br>
     * capacity: -1 ; 10; 0                                                 <br>
     *
     * src:  {notEmpty_ByteBuff && src.size() < capacity, <br>
     *      notEmpty_ByteBuff && src.size() => capacity, <br>
     *      empty_ByteBuff, null, invalidInstance} <br>
     *
     * fc: {notEmpty_FileChannel, empty_FileChannel, null, invalidInstance} <br>
     */
    @Parameterized.Parameters
    public static Collection<WriteInputTuple> getWriteInputTuples() {
        List<WriteInputTuple> writeInputTupleList = new ArrayList<>();
        //writeInputTupleList.add(new WriteInputTuple(capacity, srcSize, stateOfFc, stateOfSrc, EXPECTED));======================//
        writeInputTupleList.add(new WriteInputTuple(-1, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 0L, Exception.class));    //[1] fault of capacity < 0
        writeInputTupleList.add(new WriteInputTuple(0, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 0L, Exception.class));     //[2] fault of capacity == 0 --> FOUND BUG (No errors thrown but infinite cycle: the test passes only because of a TimeoutException is thrown)
        writeInputTupleList.add(new WriteInputTuple(0, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 0L, SUCCESS));             //[3] ON PURPOSE TO MANIFEST THE BUG
        writeInputTupleList.add(new WriteInputTuple(10, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 0L, SUCCESS));            //[4] SUCCESS
        writeInputTupleList.add(new WriteInputTuple(10, 10, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 0L, SUCCESS));           //[5] SUCCESS
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 0L, SUCCESS));           //[6] SUCCESS
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.EMPTY, 0L, SUCCESS));                //[7] SUCCESS
        writeInputTupleList.add(new WriteInputTuple(0, 0, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.EMPTY, 0L, SUCCESS));                 //[8] SUCCESS <-- even if capacity == 0 we are not writing anything, so it doesn't try to flush anything on fc
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NULL, 0L, Exception.class));         //[9] fault of stateOfSrc == NULL
        writeInputTupleList.add(new WriteInputTuple(10, 0, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.INVALID, 0L, Exception.class));      //[10] fault of stateOfSrc == INVALID
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE_OF_OBJ.NOT_EMPTY, STATE_OF_OBJ.NOT_EMPTY, 0L, SUCCESS));       //[11] SUCCESS
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE_OF_OBJ.NULL, STATE_OF_OBJ.NOT_EMPTY, 0L, Exception.class));    //[12] fault of stateOfFc == NULL
        writeInputTupleList.add(new WriteInputTuple(10, 15, STATE_OF_OBJ.INVALID, STATE_OF_OBJ.NOT_EMPTY, 0L, Exception.class)); //[13] fault of stateOfFc == INVALID
        //AFTER JACOCO REPORT:                                                                                                   //
        writeInputTupleList.add(new WriteInputTuple(10, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 3L, SUCCESS));            //[14] SUCCESS
        writeInputTupleList.add(new WriteInputTuple(10, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 6L, SUCCESS));            //[15] SUCCESS
        //AFTER PIT REPORT:                                                                                                      //
        //writeInputTupleList.add(new WriteInputTuple(10, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 5L, SUCCESS));          //[16] SUCCESS with additional flush (commenting because it is the same test as 14)
        //writeInputTupleList.add(new WriteInputTuple(10, 5, STATE_OF_OBJ.EMPTY, STATE_OF_OBJ.NOT_EMPTY, 6L, SUCCESS));          //[17] SUCCESS with additional flush (commenting because it is the same test ad 15)
        return writeInputTupleList;
    }


    /**
     * NB]: test case 129 and 133 go in an infinite loop <br>
     * --------------------------------------------------------------------<br>
     * 129] capacity = 0 | srcSize = 1 | fc = EMPTY | src = NOT_EMPTY  <br>
     * 133] capacity = 0 | srcSize = 1 | fc = NOT_EMPTY | src = NOT_EMPTY <br>
     * --------------------------------------------------------------------<br>
     * it seems you can create a 0 capacity channel so when it tries tu flush the content it
     * fails, and then it tries again and again until the test times out without giving any exception
     */
    /*
    @Parameterized.Parameters
    public static Collection<WriteInputTuple> getWriteInputTuples(){
        List<WriteInputTuple> writeInputTupleList = new ArrayList<>();
        List<Integer> capacityList = Arrays.asList(-1, 10, 0);
        List<Integer> conditionList = Arrays.asList(-1, 0, 1);
        for(Integer capacity : capacityList){
            for(Integer srcSizeCondVal : conditionList){
                for(STATE_OF_OBJ stateOfFc : STATE_OF_OBJ.values()){
                    for(STATE_OF_OBJ stateOfSrc : STATE_OF_OBJ.values()){
                        int srcSize = capacity + srcSizeCondVal;
                        if(stateOfFc == STATE_OF_OBJ.NULL ||
                                stateOfSrc == STATE_OF_OBJ.NULL ||
                                stateOfFc == STATE_OF_OBJ.INVALID ||
                                stateOfSrc == STATE_OF_OBJ.INVALID ||
                                capacity < 0 ||
                                srcSize < 0){
                            writeInputTupleList.add(new WriteInputTuple(capacity, srcSize, stateOfFc, stateOfSrc, Exception.class));
                        }else{
                            writeInputTupleList.add(new WriteInputTuple(capacity, srcSize, stateOfFc, stateOfSrc, null));
                        }
                    }
                }
            }
        }
        return writeInputTupleList;
    }

     */

    private static final class WriteInputTuple {
        private final int capacity;
        private final int srcSize;
        private final STATE_OF_OBJ stateOfFc;
        private final STATE_OF_OBJ stateOfSrc;
        private final Class<? extends Exception> expectedException;
        private final long unpersistedBytesBound;

        private WriteInputTuple(int capacity,
                                int srcSize,
                                STATE_OF_OBJ stateOfFc,
                                STATE_OF_OBJ stateOfSrc,
                                long unpersistedBytesBound,
                                Class<? extends Exception> expectedException) {
            this.capacity = capacity;
            this.srcSize = srcSize;
            this.stateOfFc = stateOfFc;
            this.stateOfSrc = stateOfSrc;
            this.unpersistedBytesBound = unpersistedBytesBound;
            this.expectedException = expectedException;
        }

        public int capacity() {
            return capacity;
        }

        public int srcSize() {
            return srcSize;
        }

        public STATE_OF_OBJ stateOfFc() {
            return stateOfFc;
        }

        public STATE_OF_OBJ stateOfSrc() {
            return stateOfSrc;
        }

        public Class<? extends Exception> expectedException() {
            return expectedException;
        }

        public long unpersistedBytesBound() {
            return unpersistedBytesBound;
        }
    }

    @BeforeClass
    public static void setUpOnce(){
        File newLogFileDirs = new File("testDir/BufChanWriteTest");
        if(!newLogFileDirs.exists()){
            newLogFileDirs.mkdirs();
        }

        File oldLogFile = new File("testDir/BufChanWriteTest/writeToThisFile.log");
        if(oldLogFile.exists()){
            oldLogFile.delete();
        }
    }

    @Before
    public void setUpEachTime(){
        try {
            Random random = new Random(System.currentTimeMillis());
            if (this.stateOfFc == STATE_OF_OBJ.NOT_EMPTY || this.stateOfFc == STATE_OF_OBJ.EMPTY) {
                if(this.stateOfFc == STATE_OF_OBJ.NOT_EMPTY) {
                    try (FileOutputStream fileOutputStream = new FileOutputStream("testDir/BufChanWriteTest/writeToThisFile.log")) {
                        this.numOfExistingBytes = random.nextInt(10);
                        byte[] alreadyExistingBytes = new byte[this.numOfExistingBytes];
                        random.nextBytes(alreadyExistingBytes);
                        fileOutputStream.write(alreadyExistingBytes);
                    }
                }
                this.fc = openNewFileChannel();
                /*
                 * fc.position(this.fc.size()) is used to set the position of the file channel (fc) to the end of the file.
                 * This operation ensures that any subsequent write operations will append data to the existing content
                 * of the file rather than overwrite it.
                 * (we did this also because StandardOpenOption.READ and .APPEND is not allowed together)
                 */
                this.fc.position(this.fc.size());
                this.data = new byte[this.srcSize];
                if(this.stateOfSrc != STATE_OF_OBJ.EMPTY){
                    random.nextBytes(this.data);
                }else{
                    Arrays.fill(data, (byte) 0);
                }
            } else if (this.stateOfFc == STATE_OF_OBJ.NULL) {
                this.fc = null;
            } else if (this.stateOfFc == STATE_OF_OBJ.INVALID) {
                this.fc = getInvalidFcInstance();
            }
            assignSrc();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assignSrc(){
        this.src = Unpooled.directBuffer(this.srcSize);
        if(this.stateOfSrc == STATE_OF_OBJ.NOT_EMPTY) {
            this.src.writeBytes(this.data);
        } else if (this.stateOfSrc == STATE_OF_OBJ.NULL) {
            this.src = null;
        } else if (this.stateOfSrc == STATE_OF_OBJ.INVALID) {
            this.src = getMockedInvalidSrcInstance();
        }
    }

    private FileChannel getInvalidFcInstance() {
        FileChannel invalidFc;
        try {
            invalidFc = openNewFileChannel();
            invalidFc.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return  invalidFc;
    }

    private static FileChannel openNewFileChannel() throws IOException {
        return FileChannel.open(Paths.get("testDir/BufChanWriteTest/writeToThisFile.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private ByteBuf getMockedInvalidSrcInstance() {
        ByteBuf invalidByteBuf = mock(ByteBuf.class);
        when(invalidByteBuf.readableBytes()).thenReturn(1);
        when(invalidByteBuf.readerIndex()).thenReturn(-1);
        return invalidByteBuf;
    }

    @After
    public void cleanupEachTime(){
        try {
            if(this.stateOfFc != STATE_OF_OBJ.NULL) {
                this.fc.close();
            }
            File oldLogFile = new File("testDir/BufChanWriteTest/writeToThisFile.log");
            if(oldLogFile.exists()){
                oldLogFile.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void cleanupOnce(){
        File newLogFileDirs = new File("testDir/BufChanWriteTest");
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

    @Test(timeout = 5000)
    public void write() throws IOException {
            BufferedChannel bufferedChannel = new BufferedChannel(this.allocator, this.fc, this.capacity, this.unpersistedBytesBound);
            bufferedChannel.write(this.src);
            /*
             * NB]: while adding entries to BufferedChannel if src has reached its capacity
             * then it will call flush method and the data gets added to the file buffer.
             */
            int expectedNumOfBytesInWriteBuff = 0;
            if(this.stateOfSrc != STATE_OF_OBJ.EMPTY && capacity!=0) {
                expectedNumOfBytesInWriteBuff = (this.srcSize < this.capacity) ? this.srcSize : this.srcSize % this.capacity;
            }
            int expectedNumOfBytesInFc = 0;
            if(this.unpersistedBytesBound > 0L){
                if(this.unpersistedBytesBound <= this.srcSize){
                    expectedNumOfBytesInFc = this.srcSize;
                    expectedNumOfBytesInWriteBuff = 0;
                }
            }else{
                expectedNumOfBytesInFc = (this.srcSize < this.capacity) ? 0 : this.srcSize - expectedNumOfBytesInWriteBuff ;
            }
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
            if(this.stateOfSrc == STATE_OF_OBJ.EMPTY){
                Assert.assertEquals("BufferedChannelPosition Check Failed", this.numOfExistingBytes, bufferedChannel.position());

            }else {
                Assert.assertEquals("BufferedChannelPosition Check Failed", this.numOfExistingBytes + this.srcSize, bufferedChannel.position());
            }
    }
}