/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.bootstrap;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import com.google.inject.ProvisionException;
import com.google.inject.spi.Message;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.airlift.bootstrap.Bootstrap.replaceEnvironmentVariables;
import static io.airlift.testing.Assertions.assertContains;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestBootstrap
{
    @Test
    public void testRequiresExplicitBindings()
    {
        Bootstrap bootstrap = new Bootstrap();
        try {
            bootstrap.initialize().getInstance(Instance.class);
            fail("should require explicit bindings");
        }
        catch (ConfigurationException e) {
            assertContains(e.getErrorMessages().iterator().next().getMessage(), "Explicit bindings are required");
        }
    }

    @Test
    public void testDoesNotAllowCircularDependencies()
    {
        Bootstrap bootstrap = new Bootstrap(binder -> {
            binder.bind(InstanceA.class);
            binder.bind(InstanceB.class);
        });

        try {
            bootstrap.initialize().getInstance(InstanceA.class);
            fail("should not allow circular dependencies");
        }
        catch (ProvisionException e) {
            assertContains(e.getErrorMessages().iterator().next().getMessage(), "circular dependencies are disabled");
        }
    }

    @Test
    public void testEnvironmentVariableReplacement()
    {
        Map<String, String> original = ImmutableMap.<String, String>builder()
                .put("apple", "apple-value")
                .put("grape", "${ENV:GRAPE}")
                .put("peach", "${ENV:PEACH}")
                .put("grass", "${ENV:!!!}")
                .put("pear", "${ENV:X_PEAR}")
                .put("cherry", "${ENV:X_CHERRY}")
                .put("orange", "orange-value")
                .put("watermelon", "${ENV:WATER}${ENV:MELON}")
                .put("blueberry", "${ENV:BLUE}${ENV:BERRY}")
                .put("contaminated-lemon", "${ENV:!!!}${ENV:LEMON}")
                .put("mixed-fruit", "mango-value:${ENV:BANANA}:${ENV:COCONUT}")
                .put("no-recursive-replacement", "${ENV:FIRST}, ${ENV:SECOND}")
                .build();

        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put("GRAPE", "env-grape")
                .put("X_CHERRY", "env-cherry")
                .put("WATER", "env-water")
                .put("MELON", "env-melon")
                .put("BERRY", "env-berry")
                .put("LEMON", "env-lemon")
                .put("BANANA", "env-banana")
                .put("COCONUT", "env-coconut")
                .put("FIRST", "env-first:${ENV:SECOND}:")
                .put("SECOND", "env-second:${ENV:FIRST}")
                .build();

        List<String> errors = new ArrayList<>();
        Map<String, String> actual = replaceEnvironmentVariables(original, environment, (key, error) -> errors.add(error));

        Map<String, String> expected = ImmutableMap.<String, String>builder()
                .put("apple", "apple-value")
                .put("grape", "env-grape")
                .put("grass", "${ENV:!!!}")
                .put("cherry", "env-cherry")
                .put("orange", "orange-value")
                .put("watermelon", "env-waterenv-melon")
                .put("contaminated-lemon", "${ENV:!!!}env-lemon")
                .put("mixed-fruit", "mango-value:env-banana:env-coconut")
                .put("no-recursive-replacement", "env-first:${ENV:SECOND}:, env-second:${ENV:FIRST}")
                .build();

        assertEquals(actual, expected);

        assertThat(errors).containsExactly(
                "Configuration property 'peach' references unset environment variable 'PEACH'",
                "Configuration property 'pear' references unset environment variable 'X_PEAR'",
                "Configuration property 'blueberry' references unset environment variable 'BLUE'");
    }

    @Test
    public void testStrictConfig()
    {
        Bootstrap bootstrap = new Bootstrap()
                .setRequiredConfigurationProperty("test-required", "foo");

        assertThatThrownBy(bootstrap::initialize)
                .isInstanceOfSatisfying(ApplicationConfigurationException.class, e ->
                        assertThat(e.getErrors()).containsExactly(
                                new Message("Configuration property 'test-required' was not used")));
    }

    @SuppressWarnings("removal")
    @Test
    public void testNonStrictConfig()
    {
        new Bootstrap()
                .setRequiredConfigurationProperty("test-required", "foo")
                .nonStrictConfig()
                .initialize();
    }

    public static class Instance {}

    public static class InstanceA
    {
        @Inject
        public InstanceA(InstanceB b) {}
    }

    public static class InstanceB
    {
        @Inject
        public InstanceB(InstanceA a) {}
    }
}
