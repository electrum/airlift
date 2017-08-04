package io.airlift.jmx.testing;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.testng.annotations.Test;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.management.MBeanServer;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTestingJmxModule
{
    @Test
    public void testTestingJmxModule()
            throws Exception
    {
        Injector injector = Guice.createInjector(new TestingJmxModule());
        MBeanServer server = injector.getInstance(MBeanServer.class);

        assertThat(server).isInstanceOf(TestingMBeanServer.class);
    }
}
