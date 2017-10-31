package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * All snippets and examples from the docs should exist as tests here so that
 * the build fails if they stop working.
 *
 * If you add a new snippet in the README, or the wiki, please also add it
 * here, so that it will be continuously tested.
 *
 * If you update this file, please also update the corresponding document
 * section.
 */
public class DocExamplesTests {
    @Test
    public void testReadme01() throws IOException {
        SafeDeleting.removeTempPath(Paths.get("tmp/uppend-test"));

        // *** START SNIPPET ***
        AppendOnlyStore db = Uppend.fileStore("tmp/uppend").build();

        db.append("my-partition", "my-key", "value-1".getBytes());
        db.append("my-partition", "my-key", "value-2".getBytes());

        db.flush();

        String values = db.readSequential("my-partition", "my-key")
                .map(String::new)
                .collect(Collectors.joining(", "));
        // value-1, value-2
        // *** END SNIPPET ***

        assertEquals("value-1, value-2", values);

        SafeDeleting.removeTempPath(Paths.get("tmp/uppend-test"));
    }
}
