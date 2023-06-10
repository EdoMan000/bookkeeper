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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Parameterized.class)
public class BufferedChannelReadingTest {
    public static final Class<? extends Exception> SUCCESS = null;
    Random random = new Random(System.currentTimeMillis());
    /**
     * Category Partitioning for fc is:<br>
     * {notEmpty, empty, null, invalidInstance}
     */
    private FileChannel fc;
    /**
     * Category Partitioning for dest is:<br>
     * {null, validInstance, invalidInstance}
     */
    private ByteBuf dest;
    /**
     * Category Partitioning for capacity is:<br>
     * {<=0, >0}
     */
    private final int capacity;
    /**
     * Category Partitioning for startingPos is:<br>
     * {<0, >=0} <br>
     * turns out like --> {< fileSize ,= fileSize, > fileSize}
     */
    private final int startingPos;
    /**
     * Category Partitioning for length is: <br>
     * {<0, >=0} <br>
     * turns out like --> {< fileSize-startingPos ,= fileSize-startingPos, > fileSize-startingPos}
     */
    private final int length;
    /**
     * Category Partitioning for fileSize is:<br>
     * {>0, =0}
     */
    private final int fileSize;
    private byte[] bytesInFileToBeRead;
    private final boolean writingBeforeReading;

    private enum STATE_OF_FC {
        EMPTY,
        NOT_EMPTY,
        NULL,
        INVALID
    }

    private enum STATE_OF_DEST {
        NULL,
        VALID,
        INVALID
    }
    private final STATE_OF_FC stateOfFc;
    private final STATE_OF_DEST stateOfDest;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public BufferedChannelReadingTest(ReadInputTuple readInputTuple) {
        this.capacity = readInputTuple.capacity();
        this.startingPos = readInputTuple.startingPos();
        this.length = readInputTuple.length();
        this.fileSize = readInputTuple.fileSize();
        this.stateOfFc = readInputTuple.stateOfFc();
        this.stateOfDest = readInputTuple.stateOfDest();
        this.writingBeforeReading = readInputTuple.writingBeforeReading();
        if(readInputTuple.expectedException() != null){
            this.expectedException.expect(readInputTuple.expectedException());
        }
    }

    /**
     * -----------------------------------------------------------------------------<br>
     * Boundary analysis:                                                             <br>
     * -----------------------------------------------------------------------------<br>
     * capacity: -1 ; 10; 0                                                           <br>
     * fc: {notEmpty_FileChannel, empty_FileChannel, null, invalidInstance}           <br>
     * dest: {null, validInstance, invalidInstance}                                   <br>
     * startingPos: 0 ; fileSize; fileSize+1                                 <br>
     * length: fileSize-startingPos-1 ; fileSize-startingPos ; fileSize-startingPos+1 <br>
     */

    @Parameterized.Parameters
    public static Collection<ReadInputTuple> getReadInputTuples() {
        List<ReadInputTuple> readInputTupleList = new ArrayList<>();
        //readInputTupleList.add(new ReadInputTuple(capacity, stateOfFc, stateOfDest, startingPos, length, fileSize, EXPECTED));============//
        readInputTupleList.add(new ReadInputTuple(-1, STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.VALID, 0, 11, 12, false, Exception.class));      //[1] fault of capacity < 0
        readInputTupleList.add(new ReadInputTuple(10, STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.VALID, 0, 11, 12, false, SUCCESS));              //[2] FOUND BUG -> it reads 12 even if i selected 11
        readInputTupleList.add(new ReadInputTuple(0, STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.VALID, 0, 11, 12, false, Exception.class));       //[3] fault of capacity = 0
        readInputTupleList.add(new ReadInputTuple(10, STATE_OF_FC.NULL, STATE_OF_DEST.VALID, 0, 11, 12, false, Exception.class));           //[4] fault of STATE_OF_FC.NULL
        readInputTupleList.add(new ReadInputTuple(10, STATE_OF_FC.INVALID, STATE_OF_DEST.VALID, 0, 11, 12, false, Exception.class));        //[5] fault of STATE_OF_FC.INVALID
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 0, 0, 0, false, SUCCESS));                   //[6] SUCCESS
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 0, 1, 0, false, Exception.class));           //[7] fault of length = fileSize-startingPos+1 -> trying to Read empty file
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.NULL, 0, 11, 12, false, Exception.class));      //[8] fault of STATE_OF_DEST.NULL
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.INVALID, 0, 11, 12, false, Exception.class));   //[9] fault of STATE_OF_DEST.INVALID
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.VALID, 12, 11, 12, false, Exception.class));    //[10] fault of startingPos = fileSize and then trying to read length > 0 bytes
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.VALID, 13, 11, 12, false, Exception.class));    //[11] fault of startingPos = fileSize + 1
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.VALID, 0, 12, 12, false, SUCCESS));             //[12] SUCCESS
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.NOT_EMPTY, STATE_OF_DEST.VALID, 0, 13, 12, false, Exception.class));     //[13] fault of length = fileSize - startingPos + 1 being more than the full content of the file
        //AFTER JACOCO REPORT                                                                                                               //
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 0, 1, 2, true, SUCCESS));                    //[14] SUCCESS even if length > fileSize -> data will be read from writeBuff FOUND BUG -> it still reads more than needed
        //AFTER PIT REPORT                                                                                                                  //
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 0, 1, 0, true, Exception.class));            //[15] fault of (like [7] but writeBeforeRead)
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 12, 11, 12, true, Exception.class));         //[16] fault of (like [10] but writeBeforeRead)
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 13, 11, 12, true, Exception.class));         //[17] fault of (like [11] but writeBeforeRead)
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 0, 13, 12, true, Exception.class));          //[18] fault of (like [13] but writeBeforeRead) <--------
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 0, 0, 0, true, SUCCESS));                    //[19] SUCCESS (like [6] but writeBeforeRead)
        readInputTupleList.add(new ReadInputTuple(10,  STATE_OF_FC.EMPTY, STATE_OF_DEST.VALID, 0, 12, 12, true, SUCCESS));                  //[20] SUCCESS (like [12] but writeBeforeRead)

        return readInputTupleList;
    }


    /*
    @Parameterized.Parameters
    public static Collection<ReadInputTuple> getReadInputTuples(){
        List<ReadInputTuple> readInputTupleList = new ArrayList<>();
        List<Integer> capacityList = Arrays.asList(-1, 10, 0);
        List<Integer> conditionList = Arrays.asList(-1, 0, 1);
        for(Integer capacity : capacityList){
            for (Integer startingPosCondVal : conditionList) {
                for (Integer lengthCondVal : conditionList) {
                    for (STATE_OF_FC stateOfFc : STATE_OF_FC.values()) {
                        for (STATE_OF_DEST stateOfDest : STATE_OF_DEST.values()) {
                            int fileSize = 0;
                            if(stateOfFc == STATE_OF_FC.NOT_EMPTY){
                                Random random = new Random(System.currentTimeMillis());
                                do {
                                    fileSize = random.nextInt(30);
                                }while (fileSize == 0);
                            }
                            int startingPos = fileSize + startingPosCondVal;
                            int length = (fileSize - startingPos) + lengthCondVal;
                            if (stateOfFc == STATE_OF_FC.NULL ||
                                    stateOfFc == STATE_OF_FC.INVALID ||
                                    length > 0 && stateOfDest == STATE_OF_DEST.NULL ||
                                    length > 0 && stateOfDest == STATE_OF_DEST.INVALID ||
                                    capacity < 0 ||
                                    length > 0 && startingPos < 0 ||
                                    length > 0 && stateOfFc == STATE_OF_FC.EMPTY ||
                                    length > 0 && startingPos + length > fileSize && stateOfFc == STATE_OF_FC.NOT_EMPTY ||
                                    length > 0 && capacity == 0 && startingPos + length >= fileSize && stateOfFc == STATE_OF_FC.NOT_EMPTY){
                                readInputTupleList.add(new ReadInputTuple(capacity, stateOfFc, stateOfDest, startingPos, length, fileSize, Exception.class));
                            } else {
                                readInputTupleList.add(new ReadInputTuple(capacity, stateOfFc, stateOfDest, startingPos, length, fileSize, null));
                            }
                        }
                    }
                }
            }
        }
        return readInputTupleList;
    }
     */

    private static final class ReadInputTuple {
        private final int capacity;
        private final STATE_OF_FC stateOfFc;
        private final STATE_OF_DEST stateOfDest;
        private final int startingPos;
        private final int length;
        private final int fileSize;
        private final boolean writingBeforeReading;
        private final Class<? extends Exception> expectedException;

        private ReadInputTuple(int capacity,
                               STATE_OF_FC stateOfFc,
                               STATE_OF_DEST stateOfDest,
                               int startingPos,
                               int length,
                               int fileSize,
                               boolean writingBeforeReading,
                               Class<? extends Exception> expectedException) {
            this.capacity = capacity;
            this.stateOfFc = stateOfFc;
            this.stateOfDest = stateOfDest;
            this.startingPos = startingPos;
            this.length = length;
            this.fileSize = fileSize;
            this.writingBeforeReading = writingBeforeReading;
            this.expectedException = expectedException;
        }

        public int capacity() {
            return capacity;
        }

        public STATE_OF_FC stateOfFc() {
            return stateOfFc;
        }

        public STATE_OF_DEST stateOfDest() {
            return stateOfDest;
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

        public boolean writingBeforeReading() {
            return writingBeforeReading;
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
                this.bytesInFileToBeRead = new byte[this.fileSize];
                random.nextBytes(this.bytesInFileToBeRead);
                if(this.stateOfFc == STATE_OF_FC.NOT_EMPTY) {
                    try (FileOutputStream fileOutputStream = new FileOutputStream("testDir/BufChanReadTest/readFromThisFile.log")) {
                        fileOutputStream.write(this.bytesInFileToBeRead);
                    }
                }
                this.fc = openNewFileChannel();
                this.fc.position(this.fc.size());
            } else if (this.stateOfFc == STATE_OF_FC.NULL) {
                this.fc = null;
            } else if (this.stateOfFc == STATE_OF_FC.INVALID) {
                this.fc = getInvalidFcInstance();
            }
            assignDest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assignDest() {
        this.dest = Unpooled.buffer();
        if (this.stateOfDest == STATE_OF_DEST.NULL) {
            this.dest = null;
        } else if (this.stateOfDest == STATE_OF_DEST.INVALID) {
            this.dest = getMockedInvalidDestInstance();
        }
    }

    private ByteBuf getMockedInvalidDestInstance() {
        ByteBuf invalidByteBuf = mock(ByteBuf.class);
        when(invalidByteBuf.writableBytes()).thenReturn(1);
        when(invalidByteBuf.writeBytes(any(ByteBuf.class), any(int.class), any(int.class) )).thenThrow(new IndexOutOfBoundsException("Hi, i'm an invalid instance!"));
        return invalidByteBuf;
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


    @Test
    public void read() throws IOException {
        BufferedChannel bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fc, this.capacity);
        if(this.writingBeforeReading){
            ByteBuf tempByteBuf = Unpooled.buffer();
            tempByteBuf.writeBytes(this.bytesInFileToBeRead);
            bufferedChannel.write(tempByteBuf);
        }
        Integer actualNumOfBytesRead = bufferedChannel.read(this.dest, this.startingPos, this.length);
        Integer expectedNumOfBytesInReadBuff = 0;
        byte[] expectedBytes = new byte[0];
        if (this.startingPos <= this.fc.size()) {
            if(this.length > 0) {
                if(writingBeforeReading){
                    expectedNumOfBytesInReadBuff = Math.toIntExact((this.length+1 - this.startingPos >= this.length) ? this.length : this.length+1 - this.startingPos - this.length);
                    expectedBytes = Arrays.copyOfRange(this.bytesInFileToBeRead, this.startingPos, this.startingPos + expectedNumOfBytesInReadBuff);
                }else {
                    expectedNumOfBytesInReadBuff = Math.toIntExact((this.fc.size() - this.startingPos >= this.length) ? this.length : this.fc.size() - this.startingPos - this.length);
                    expectedBytes = Arrays.copyOfRange(this.bytesInFileToBeRead, this.startingPos, this.startingPos + expectedNumOfBytesInReadBuff);
                }
            }
        }
        byte[] actualBytesRead = new byte[0];
        if(this.stateOfDest == STATE_OF_DEST.VALID) {
            actualBytesRead = Arrays.copyOfRange(this.dest.array(), 0, actualNumOfBytesRead);
        }

        Assert.assertEquals("BytesRead Check Failed", Arrays.toString(expectedBytes), Arrays.toString(actualBytesRead));
        Assert.assertEquals("NumOfBytesRead Check Failed", expectedNumOfBytesInReadBuff, actualNumOfBytesRead);
    }
}