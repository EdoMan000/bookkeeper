package org.apache.bookkeeper.bookie;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(value = Parameterized.class)
public class JournalChannelTest {

    private final long size;
    
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private final File journalDir;
    private final long logId;

    public JournalChannelTest(InputTuple inputTuple) {
        this.size = inputTuple.size();
        this.journalDir = inputTuple.journalDir();
        this.logId = inputTuple.logId();
        if(inputTuple.expectedException() != null){
            this.expectedException.expect(inputTuple.expectedException());
        }
    }

    private static final class InputTuple {
        private final long size;
        private final File journalDir;
        private final long logId;
        private final Class<? extends Exception> expectedException;

        private InputTuple(long size,
                                File journalDir,
                                long logId,
                                Class<? extends Exception> expectedException) {
            this.size = size;
            this.journalDir = journalDir;
            this.logId = logId;
            this.expectedException = expectedException;
        }

        public long size() {
            return size;
        }

        public File journalDir() {
            return journalDir;
        }

        public long logId() {
            return logId;
        }

        public Class<? extends Exception> expectedException() {
            return expectedException;
        }

    }


    @Test
    public void preAllocIfNeededTest() throws IOException {
        JournalChannel journalChannel = new JournalChannel(this.journalDir, this.logId);
        journalChannel.preAllocIfNeeded(this.size);


    }
    
    @Parameterized.Parameters
    public static Collection<InputTuple> getInputTuples() {
        List<InputTuple> inputTupleList = new ArrayList<>();
        //inputTupleList.add(new InputTuple());
        return  inputTupleList;
    }
    
}