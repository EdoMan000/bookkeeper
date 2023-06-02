package org.apache.bookkeeper.common.component;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LifecycleTest {

    Lifecycle lifecycle = mock(Lifecycle.class);
    @Before
    public void before(){
        when(lifecycle.state()).thenReturn(Lifecycle.State.CLOSED);
    }

    @Test
    public void anotherTest() {
        Assert.assertEquals(lifecycle.state(), Lifecycle.State.CLOSED);
    }

    @Test
    public void moveToStartedTest() {
        Lifecycle lifecycle = new Lifecycle();
        Assert.assertTrue(lifecycle.moveToStarted());
    }

    @Test
    public void moveToStoppedTest() {
        Lifecycle lifecycle = new Lifecycle();
        Assert.assertFalse(lifecycle.moveToStopped());
    }
}