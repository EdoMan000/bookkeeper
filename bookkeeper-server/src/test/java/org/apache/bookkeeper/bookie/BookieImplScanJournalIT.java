package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * NB]: in this integration test we are assuming that both the involved classes were already successfully unit tested
 */
public class BookieImplScanJournalIT {

    private BookieImpl bookieImpl;
    private Journal journal;
    private Journal.JournalScanner scanner;
    private static final Logger LOG = LoggerFactory.getLogger(BookieImplScanJournalIT.class);

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
        this.scanner = getMockedScanner();
        this.journal = this.bookieImpl.journals.get(0);
        this.journal = spy(this.journal);
        //End of initialization of journal e bookie=====================================================================
    }

    /**
     * This test is to assert that the class Journal is reachable by BookieImpl
     * when trying to scan journals during a replay() action
     */
    @Test//@Ignore
    public void reachabilityTest() {
        try {
            invokeReplayPrivateFunction();
            verify(this.journal, times(1)).scanJournal(anyLong(), anyLong(), eq(this.scanner));
        } catch (Exception e) {
            Assert.fail("Exception while trying to test reachability of method call scanJournal() during IT");
        }
    }

    /**
     * if replay() is called with an actual instance of Journal and with a valid scanner (always valid as it is mocked)
     * the expected behaviour is that the journal is scanned (scanner processes at least once) and
     * the logMark of the journal is updated with the new offset
     */
    @Test//@Ignore
    public void expectedBehaviourJournalTest() {
        try {
            long oldLogFileOffset = this.journal.getLastLogMark().getCurMark().logFileOffset;
            invokeReplayPrivateFunction();
            verify(this.journal, times(1)).scanJournal(anyLong(), anyLong(), eq(this.scanner));
            verify(this.scanner, atLeastOnce()).process(anyInt(), anyLong(), any(ByteBuffer.class));
            long newLogFileOffset = this.journal.getLastLogMark().getCurMark().logFileOffset;
            Assert.assertTrue(newLogFileOffset > oldLogFileOffset);
        } catch (Exception e) {
            Assert.fail("Exception while trying to test expected behaviour of method call scanJournal() during IT");
        }
    }

    private Journal.JournalScanner getMockedScanner() throws IOException {
        Journal.JournalScanner journalScanner = mock(Journal.JournalScanner.class);
        doAnswer(invocation -> {
            int journalVersion = invocation.getArgument(0);
            long offset = invocation.getArgument(1);
            ByteBuffer entry = invocation.getArgument(2);
            LOG.warn("Journal Version: {} | Offset: {} | Entry: {}", journalVersion, offset, entry);
            return null;
        }).when(journalScanner).process(anyInt(), anyLong(), any(ByteBuffer.class));
        return  journalScanner;
    }


    private void invokeReplayPrivateFunction() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        //using reflection once again to invoke private method replay() in BookieImpl
        Method method = BookieImpl.class.getDeclaredMethod("replay", Journal.class, Journal.JournalScanner.class);
        method.setAccessible(true);
        method.invoke(this.bookieImpl, this.journal, this.scanner);

    }

    @After
    public void cleanupEachTime() {
        this.bookieImpl.shutdown();

        for (File dir : tempDirs) {
            FileUtils.deleteQuietly(dir);
        }
        tempDirs.clear();
    }

    final List<File> tempDirs = new ArrayList<>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

}
