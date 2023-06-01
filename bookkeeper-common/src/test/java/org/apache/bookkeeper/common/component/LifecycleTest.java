package org.apache.bookkeeper.common.component;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
public class LifecycleTest {

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