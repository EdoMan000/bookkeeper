package org.apache.bookkeeper.common.component;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
public class LifecycleTest {

    @Test
    public void state() {
        Lifecycle lifecycle = new Lifecycle();
        Lifecycle lifecycle2 = new Lifecycle();
        Assert.assertEquals(lifecycle.state(), lifecycle2.state());
    }
}