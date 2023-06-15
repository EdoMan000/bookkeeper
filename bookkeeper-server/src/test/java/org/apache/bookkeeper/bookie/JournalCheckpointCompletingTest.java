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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class JournalCheckpointCompletingTest {
    public static final Class<? extends Exception> SUCCESS = null;
    private Journal journal;
    private BookieImpl bookieImpl;
    /**
     * Category Partitioning for checkpoint is:<br>
     * {null}, {validCheckPoint}, {invalidCheckpoint}
     */
    private CheckpointSource.Checkpoint checkpoint;
    private final STATE_OF_CHECKPOINT stateOfCheckpoint;
    /**
     * Category Partitioning for scanner is:<br>
     * {true}, {false}
     */
    private final boolean compact;
    private final boolean moreJournalsInDirectory;
    private enum STATE_OF_CHECKPOINT {
        NULL,
        VALID,
        INVALID
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public JournalCheckpointCompletingTest(InputTuple inputTuple) {
        this.stateOfCheckpoint = inputTuple.stateOfCheckpoint();
        this.compact = inputTuple.compact();
        this.moreJournalsInDirectory = inputTuple.moreJournalsInDirectory();
    }

    private static final class InputTuple {
        private final STATE_OF_CHECKPOINT stateOfCheckpoint;
        private final boolean compact;
        private final boolean moreJournalsInDirectory;

        private InputTuple(STATE_OF_CHECKPOINT stateOfCheckpoint,
                           boolean compact,
                           boolean moreJournalsInDirectory) {
            this.stateOfCheckpoint = stateOfCheckpoint;
            this.compact = compact;
            this.moreJournalsInDirectory = moreJournalsInDirectory;
        }
        public STATE_OF_CHECKPOINT stateOfCheckpoint(){
            return stateOfCheckpoint;
        }

        public boolean compact() {
            return compact;
        }
        public boolean moreJournalsInDirectory() {
            return moreJournalsInDirectory;
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
        JournalWriter.writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100,
                    "Hey, This is a test!".getBytes());
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);
        this.bookieImpl = new TestBookieImpl(conf);
        this.journal = bookieImpl.journals.get(0);
        //End of initialization of journal e bookie=====================================================================
        switch (this.stateOfCheckpoint) {
            case NULL:
                this.checkpoint = null;
                break;
            case VALID:
                //ADDITION AFTER JACOCO REPORT TO BYPASS FILTER ON listJournalIds
                if(this.moreJournalsInDirectory) {
                    this.journal.setLastLogMark(2000000000000L, 0L);
                }
                this.checkpoint = this.journal.newCheckpoint();
                break;
            case INVALID:
                this.checkpoint = CheckpointSource.DEFAULT.newCheckpoint();
                break;
        }
        if(this.moreJournalsInDirectory){
            for (int i = 1; i <= 10; i++) {
                JournalWriter.writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100,
                        "Hey, This is a test!".getBytes());
            }
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
     * checkpoint: null, validInstance, invalidInstance                             <br>
     * compact: true, false                                                   <br>
     */
    @Parameterized.Parameters
    public static Collection<InputTuple> getInputTuples() {
        List<InputTuple> inputTupleList = new ArrayList<>();
        //inputTupleList.add(new InputTuple(stateOfCheckpoint, compact, moreJournalsInDirectory, expectedException));
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.NULL, false, false));       // [1]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.VALID, false, false));      // [2]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.INVALID, false, false));    // [3]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.NULL, true, false));        // [4]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.VALID, true, false));       // [5]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.INVALID, true, false));     // [6]
        //AFTER JACOCO REPORT                                                             //
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.NULL, false, true));        // [7]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.VALID, false, true));       // [8]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.INVALID, false, true));     // [9]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.NULL, true, true));         // [10]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.VALID, true, true));        // [11]
        inputTupleList.add(new InputTuple(STATE_OF_CHECKPOINT.INVALID, true, true));      // [12]
        return  inputTupleList;
    }

    @Test//@Ignore
    public void checkpointComplete() throws IOException, NoSuchFieldException, IllegalAccessException {
        //ADDITION AFTER PIT REPORT
        if(this.stateOfCheckpoint == STATE_OF_CHECKPOINT.VALID && this.compact && this.moreJournalsInDirectory){
            Logger mockLogger = mock(Logger.class);
            Field field = Journal.class.getDeclaredField("LOG");
            field.setAccessible(true);
            field.set(null, mockLogger);
            this.journal.checkpointComplete(this.checkpoint , true);
            verify(mockLogger, atLeastOnce()).info(any());
            field.set(null, LoggerFactory.getLogger(Journal.class));
        }else{
            this.journal.checkpointComplete(this.checkpoint , this.compact);
        }
    }

    final List<File> tempDirs = new ArrayList<>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }
}