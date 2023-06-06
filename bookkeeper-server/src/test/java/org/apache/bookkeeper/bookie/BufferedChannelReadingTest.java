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
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

@RunWith(value = Parameterized.class)
public class BufferedChannelReadingTest {
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
     * {<0, >0 ,=0}
     */
    private final int capacity;
    /**
     * Category Partitioning for startingPos is:<br>
     * {<0, >0 ,=0} <br>
     * turns out like --> {< fileSize ,= fileSize, > fileSize}
     */
    private final int startingPos;
    /**
     * Category Partitioning for length is: <br>
     * {<0, >0 ,=0} <br>
     * turns out like --> {< fileSize-startingPos ,= fileSize-startingPos, > fileSize-startingPos}
     */
    private final int length;
    /**
     * Category Partitioning for fileSize is:<br>
     * {<0, >0 ,=0}
     */
    private final int fileSize;
    private byte[] bytesInFileToBeRead;

    private enum STATE_OF_FC {
        EMPTY,
        NOT_EMPTY,
        NULL,
        INVALID
    }
    private final STATE_OF_FC stateOfFc;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public BufferedChannelReadingTest(ReadInputTuple readInputTuple) {
        this.capacity = readInputTuple.capacity();
        this.startingPos = readInputTuple.startingPos();
        this.length = readInputTuple.length();
        this.fileSize = readInputTuple.fileSize();
        this.stateOfFc = readInputTuple.stateOfFc();
        if(readInputTuple.expectedException() != null){
            this.expectedException.expect(readInputTuple.expectedException());
        }
    }

    /**
     * -----------------------------------------------------------------------------<br>
     * Boundary analysis:                                                             <br>
     * -----------------------------------------------------------------------------<br>
     * capacity: -1 ; 10; 0                                                           <br>
     * startingPos: fileSize-1 ; fileSize; fileSize+1                                 <br>
     * length: fileSize-startingPos-1 ; fileSize-startingPos ; fileSize-startingPos+1 <br>
     * fileSize: -1 ; 11; 0                                                           <br>
     * fc: {notEmpty_FileChannel, empty_FileChannel, null, invalidInstance}           <br>
     */

    @Parameterized.Parameters
    public static Collection<ReadInputTuple> getReadInputTuples(){
        List<ReadInputTuple> readInputTupleList = new ArrayList<>();
        List<Integer> capacityList = Arrays.asList(-1, 10, 0);
        List<Integer> conditionList = Arrays.asList(-1, 0, 1);
        List<Integer> fileSizeList = Arrays.asList(-1, 11, 0);
        for(Integer fileSize : fileSizeList){
            for(Integer capacity : capacityList){
                for(Integer startingPosCondVal : conditionList){
                    for(Integer lengthCondVal : conditionList){
                        for(STATE_OF_FC stateOfFc: STATE_OF_FC.values()){
                            int startingPos = fileSize + startingPosCondVal;
                            int length = (fileSize - startingPos) + lengthCondVal;
                            if(stateOfFc == STATE_OF_FC.NULL ||
                                    stateOfFc == STATE_OF_FC.INVALID ||
                                    capacity < 0 ||
                                    (startingPos > length && length > 0 && startingPos+length > fileSize && stateOfFc==STATE_OF_FC.NOT_EMPTY) ||
                                    (capacity == 0 && startingPos > length && length > 0 && startingPos+length >= fileSize && stateOfFc==STATE_OF_FC.NOT_EMPTY) ||
                                    (startingPos > length && length > 0 && startingPos+length >= fileSize && stateOfFc==STATE_OF_FC.EMPTY) ||
                                    (startingPos < 0 && length > 0) ||
                                    length > 0 && fileSize == 0){
                                readInputTupleList.add(new ReadInputTuple(capacity, startingPos, length, fileSize, stateOfFc, Exception.class));
                            }else{
                                readInputTupleList.add(new ReadInputTuple(capacity, startingPos, length, fileSize, stateOfFc, null));
                            }
                        }
                    }
                }
            }
        }
        readInputTupleList.add(new ReadInputTuple(0, 1, 0, 0, STATE_OF_FC.EMPTY, null));
        return readInputTupleList;
    }

    private static final class ReadInputTuple {
        private final int capacity;
        private final int startingPos;
        private final int length;
        private final int fileSize;
        private final STATE_OF_FC stateOfFc;
        private final Class<? extends Exception> expectedException;

        private ReadInputTuple(int capacity,
                               int startingPos,
                               int length,
                               int fileSize,
                               STATE_OF_FC stateOfFc,
                               Class<? extends Exception> expectedException) {
            this.capacity = capacity;
            this.startingPos = startingPos;
            this.length = length;
            this.fileSize = fileSize;
            this.stateOfFc = stateOfFc;
            this.expectedException = expectedException;
        }

        public int capacity() {
            return capacity;
        }

        public int startingPos() {
            return startingPos;
        }

        public int length() {
            return length;
        }

        public int fileSize() {
            return fileSize;
        }

        public STATE_OF_FC stateOfFc() {
            return stateOfFc;
        }

        public Class<? extends Exception> expectedException() {
            return expectedException;
        }

        }

    @BeforeClass
    public static void setUpOnce(){
        File newLogFileDirs = new File("testDir/BufChanReadTest");
        if(!newLogFileDirs.exists()){
            newLogFileDirs.mkdirs();
        }

        File oldLogFile = new File("testDir/BufChanReadTest/readFromThisFile.log");
        if(oldLogFile.exists()){
            oldLogFile.delete();
        }
    }

    @Before
    public void setUpEachTime(){
        try {
            if (this.stateOfFc == STATE_OF_FC.NOT_EMPTY || this.stateOfFc == STATE_OF_FC.EMPTY) {
                if(this.stateOfFc == STATE_OF_FC.NOT_EMPTY) {
                    Random random = new Random();
                    try (FileOutputStream fileOutputStream = new FileOutputStream("testDir/BufChanReadTest/readFromThisFile.log")) {
                        if(this.fileSize > 0) {
                            this.bytesInFileToBeRead = new byte[this.fileSize];
                            random.nextBytes(this.bytesInFileToBeRead);
                            fileOutputStream.write(this.bytesInFileToBeRead);
                        }else{
                            this.bytesInFileToBeRead = new byte[0];
                        }
                    }
                }
                this.fc = openNewFileChannel();
                this.fc.position(this.fc.size());
            } else if (this.stateOfFc == STATE_OF_FC.NULL) {
                this.fc = null;
            } else if (this.stateOfFc == STATE_OF_FC.INVALID) {
                this.fc = getInvalidFcInstance();
            }
        } catch (IOException e) {
            e.printStackTrace();
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
        return FileChannel.open(Paths.get("testDir/BufChanReadTest/readFromThisFile.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @After
    public void cleanupEachTime(){
        try {
            if(this.stateOfFc != STATE_OF_FC.NULL) {
                this.fc.close();
            }
            File oldLogFile = new File("testDir/BufChanReadTest/readFromThisFile.log");
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


    @Test//(timeout = 5000)
    public void read() throws IOException {
        BufferedChannel bufferedChannel = new BufferedChannel(this.allocator, this.fc, this.capacity);
        ByteBuf dest = Unpooled.buffer();
        Integer actualNumOfBytesRead = bufferedChannel.read(dest, this.startingPos, this.length);
        Integer expectedNumOfBytesInReadBuff = 0;
        byte[] expectedBytes = new byte[0];
        if (this.startingPos <= this.fileSize) {
            if(this.length > 0) {
                expectedNumOfBytesInReadBuff = (this.fileSize - this.startingPos >= this.length) ? this.length : this.fileSize - this.startingPos - this.length;
                expectedBytes = Arrays.copyOfRange(this.bytesInFileToBeRead, this.startingPos, this.startingPos + expectedNumOfBytesInReadBuff);
            }
        }
        byte[] actualBytesRead = Arrays.copyOfRange(dest.array(), 0, actualNumOfBytesRead);

        Assert.assertEquals("BytesRead Check Failed", Arrays.toString(expectedBytes), Arrays.toString(actualBytesRead));
        Assert.assertEquals("NumOfBytesRead Check Failed", expectedNumOfBytesInReadBuff, actualNumOfBytesRead);
    }
}