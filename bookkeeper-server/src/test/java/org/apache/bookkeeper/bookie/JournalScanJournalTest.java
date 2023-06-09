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

@RunWith(value = Parameterized.class)
public class JournalScanJournalTest {
    /**
     * Category Partitioning for journalId is:<br>
     * {<0, >=0}
     * --> turns out this can be either equal or different from the actual journalId
     * so categoory partitioning would be {correcJournalId, incorrectJournalId}
     */
    private long journalId;
    /**
     * Category Partitioning for journalPos is:<br>
     * {<0, >=0}
     */
    private final long journalPos;
    /**
     * Category Partitioning for scanner is:<br>
     * {null, validScanner, invalidSanner}
     */
    private Journal.JournalScanner scanner;
    private BookieImpl bookieImpl;
    private Journal journal;
    private final STATE_OF_SCANNER stateOfScanner;
    private final STATE_OF_JOURNAL_ID stateOfJournalId;

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
    private static final Logger LOG = LoggerFactory.getLogger(JournalScanJournalTest.class);

    public JournalScanJournalTest(InputTuple inputTuple) {
        this.stateOfJournalId = inputTuple.stateOfJournalId();
        this.journalPos = inputTuple.journalPos();
        this.stateOfScanner = inputTuple.stateOfScanner();
        if(inputTuple.expectedException() != null){
            this.expectedException.expect(inputTuple.expectedException());
        }
    }

    private static final class InputTuple {
        private final STATE_OF_JOURNAL_ID stateOfJournalId;
        private final long journalPos;
        private final STATE_OF_SCANNER stateOfScanner;
        private final Class<? extends Exception> expectedException;

        private InputTuple(STATE_OF_JOURNAL_ID stateOfJournalId,
                           long journalPos,
                           STATE_OF_SCANNER stateOfScanner,
                           Class<? extends Exception> expectedException) {
            this.journalPos = journalPos;
            this.stateOfScanner = stateOfScanner;
            this.stateOfJournalId = stateOfJournalId;
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
        JournalV4Writer.writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100,
                "Hey, This is a test!".getBytes());

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
            this.scanner = new ValidScanner();
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


    //@Test
    public void scanJournalTest() throws IOException {
        long actualScanOffset = this.journal.scanJournal(this.journalId, this.journalPos, this.scanner);

        Assert.assertTrue("ScanOffsetCheck Failed", actualScanOffset > 0);


    }

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