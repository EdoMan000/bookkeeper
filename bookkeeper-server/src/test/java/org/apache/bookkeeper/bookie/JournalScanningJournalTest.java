package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class JournalScanningJournalTest {
    public static final Class<? extends Exception> SUCCESS = null;
    /**
     * Category Partitioning for journalId is:<br>
     * {<0}, {>=0}
     * --> turns out this can be either equal or different from the actual journalId
     * so categoory partitioning would be {correcJournalId, incorrectJournalId}
     */
    private long journalId;
    /**
     * Category Partitioning for journalPos is:<br>
     * {<0}, {>=0}
     */
    private final long journalPos;
    /**
     * Category Partitioning for scanner is:<br>
     * {null}, {validScanner}, {invalidSanner}
     */
    private Journal.JournalScanner scanner;
    private BookieImpl bookieImpl;
    private Journal journal;
    private final STATE_OF_SCANNER stateOfScanner;
    private final STATE_OF_JOURNAL_ID stateOfJournalId;
    private final boolean journalV5Version;

    private enum STATE_OF_SCANNER {
        NULL,
        VALID,
        INVALID
    }

    private enum STATE_OF_JOURNAL_ID {
        CORRECT,
        INCORRECT
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private static final Logger LOG = LoggerFactory.getLogger(JournalScanningJournalTest.class);

    public JournalScanningJournalTest(InputTuple inputTuple) {
        this.stateOfJournalId = inputTuple.stateOfJournalId();
        this.journalPos = inputTuple.journalPos();
        this.stateOfScanner = inputTuple.stateOfScanner();
        this.journalV5Version = inputTuple.journalV5Version();
        if(inputTuple.expectedException() != null){
            this.expectedException.expect(inputTuple.expectedException());
        }
    }

    private static final class InputTuple {
        private final STATE_OF_JOURNAL_ID stateOfJournalId;
        private final long journalPos;
        private final STATE_OF_SCANNER stateOfScanner;
        private final Class<? extends Exception> expectedException;
        private boolean journalV5Version;

        private InputTuple(STATE_OF_JOURNAL_ID stateOfJournalId,
                           long journalPos,
                           STATE_OF_SCANNER stateOfScanner,
                           boolean journalV5Version,
                           Class<? extends Exception> expectedException) {
            this.journalPos = journalPos;
            this.stateOfScanner = stateOfScanner;
            this.stateOfJournalId = stateOfJournalId;
            this.journalV5Version = journalV5Version;
            this.expectedException = expectedException;
        }

        public STATE_OF_JOURNAL_ID stateOfJournalId() {
            return stateOfJournalId;
        }

        public long journalPos() {
            return journalPos;
        }

        public STATE_OF_SCANNER stateOfScanner() {
            return stateOfScanner;
        }

        public Class<? extends Exception> expectedException() {
            return expectedException;
        }

        public boolean journalV5Version() {
            return journalV5Version;
        }
    }

    @Before
    public void setUpEachTime() throws Exception {
        //Initialization of journal e bookie============================================================================
        // (we use API and procedures offered by test classes or util classes that were
        //  already present in the testing suite of BOOKKEEPER)
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));
        if(this.journalV5Version){
            JournalWriter.writeV5Journal(BookieImpl.getCurrentDirectory(journalDir), 2 * JournalChannel.SECTOR_SIZE,
                    "Hey, This is a test V5!".getBytes());
        }else {
            JournalWriter.writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100,
                    "Hey, This is a test V4!".getBytes());
        }

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);

        this.bookieImpl = new TestBookieImpl(conf);
        this.journal = bookieImpl.journals.get(0);
        //End of initialization of journal e bookie=====================================================================
        if (this.stateOfJournalId == STATE_OF_JOURNAL_ID.CORRECT) {
            this.journalId = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
        } else if (this.stateOfJournalId == STATE_OF_JOURNAL_ID.INCORRECT) {
            this.journalId = 2*Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
        }
        if(this.stateOfScanner == STATE_OF_SCANNER.NULL) {
            this.scanner = null;
        } else if (this.stateOfScanner == STATE_OF_SCANNER.VALID) {
            //adding spy for pit mutation coverage increase
            this.scanner = spy(new ValidScanner());
        } else if (this.stateOfScanner == STATE_OF_SCANNER.INVALID) {
            this.scanner = new InvalidScanner();
        }
    }

    @After
    public void cleanupEachTime() {
        this.bookieImpl.shutdown();

        for (File dir : tempDirs) {
            FileUtils.deleteQuietly(dir);
        }
        tempDirs.clear();
    }

    /**
     * -----------------------------------------------------------------------------<br>
     * Boundary analysis:                                                             <br>
     * -----------------------------------------------------------------------------<br>
     * journalId: correctJournalId; incorrectJournalId                             <br>
     * journalPos: -1; 10; 0                                                       <br>
     * scanner: null, validInstance, invalidInstance                             <br>
     */
    @Parameterized.Parameters
    public static Collection<InputTuple> getInputTuples() {
        List<InputTuple> inputTupleList = new ArrayList<>();
        //inputTupleList.add(new InputTuple(stateOfJournalId, journalPos, stateOfScanner, JournalV5, EXPECTED));==================//
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 10, STATE_OF_SCANNER.VALID, false, SUCCESS));             // [1] SUCCESS
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 10, STATE_OF_SCANNER.INVALID, false, Exception.class));  // [2] fault of STATE_OF_SCANNER==INVALID
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 10, STATE_OF_SCANNER.NULL, false, Exception.class));    // [3] fault of STATE_OF_SCANNER==NULL
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.INCORRECT, 10, STATE_OF_SCANNER.VALID, false, SUCCESS));        // [4] No Exception -> still scans something for some reason (black boxing it)
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, -1, STATE_OF_SCANNER.VALID, false, SUCCESS));         // [5] No Exception -> journalPos <= 0 doesn't throw exception
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 0, STATE_OF_SCANNER.VALID, false, SUCCESS));         // [6] No Exception -> journalPos <= 0 doesn't throw exception
                                                                                                                           //
        //AFTER JACOCO REPORT -> JOURNAL V5                                                                               //
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 10, STATE_OF_SCANNER.VALID, true, SUCCESS));      // [7] SUCCESS
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 10, STATE_OF_SCANNER.INVALID, true, SUCCESS));   // [8] No Exception ->  fault of STATE_OF_SCANNER==INVALID is not bothering because there is padding record and it is not processed
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 10, STATE_OF_SCANNER.NULL, true, SUCCESS));     // [9] No Exception ->  fault of STATE_OF_SCANNER==NULL is not bothering because there is padding record and it is not processed
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.INCORRECT, 10, STATE_OF_SCANNER.VALID, true, SUCCESS)); // [10] No Exception -> still scans something for some reason (black boxing it)
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, -1, STATE_OF_SCANNER.VALID, true, SUCCESS));  // [11] No Exception -> journalPos <= 0 doesn't throw exception
        inputTupleList.add(new InputTuple(STATE_OF_JOURNAL_ID.CORRECT, 0, STATE_OF_SCANNER.VALID, true, SUCCESS));  // [12] No Exception -> journalPos <= 0 doesn't throw exception
        return  inputTupleList;                                                                                    //
    }




    /*
    @Parameterized.Parameters
    public static Collection<InputTuple> getInputTuples() {
        List<Long> journalPosList = Arrays.asList((long)-1, (long)10, (long)0);
        List<InputTuple> inputTupleList = new ArrayList<>();
        for(long journalPos: journalPosList){
            for(STATE_OF_SCANNER stateOfScanner : STATE_OF_SCANNER.values()) {
                for(STATE_OF_JOURNAL_ID stateOfJournalId: STATE_OF_JOURNAL_ID.values()) {
                    if(stateOfJournalId == STATE_OF_JOURNAL_ID.INCORRECT){
                        inputTupleList.add(new InputTuple(stateOfJournalId, journalPos, stateOfScanner, null));
                    }else{
                        if (stateOfScanner == STATE_OF_SCANNER.NULL || stateOfScanner == STATE_OF_SCANNER.INVALID) {// || journalPos < 0 || stateOfJournalId == STATE_OF_JOURNAL_ID.INCORRECT){
                            inputTupleList.add(new InputTuple(stateOfJournalId, journalPos, stateOfScanner, Exception.class));
                        } else {
                            inputTupleList.add(new InputTuple(stateOfJournalId, journalPos, stateOfScanner, null));
                        }
                    }
                }
            }
        }
        return  inputTupleList;
    }

     */

    @Test
    public void scanJournalTest() throws IOException {
        long actualScanOffset = this.journal.scanJournal(this.journalId, this.journalPos, this.scanner);

        Assert.assertTrue("ScanOffsetCheck Failed", actualScanOffset > 0);
        //PIT ADDITION
        if(this.stateOfScanner == STATE_OF_SCANNER.VALID && this.stateOfJournalId == STATE_OF_JOURNAL_ID.CORRECT && this.journalPos<=0) {
            if (this.journalV5Version) {
                verify(this.scanner, atLeastOnce()).process(eq(5), eq((long)512), any());
            } else {
                verify(this.scanner, atLeastOnce()).process(eq(4), eq((long)8), any());
            }
        }

    }

    private static class ValidScanner implements Journal.JournalScanner{
        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            LOG.warn("Journal Version : {} | Offset : {} | Entry : {}", journalVersion, offset, entry);
        }
    }

    private static class InvalidScanner implements Journal.JournalScanner{
        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
            throw new IOException("Hi, i'm an invalidScanner!");
        }
    }

    final List<File> tempDirs = new ArrayList<>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

}