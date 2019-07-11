package util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;

public class ConstantsTest {

    @Before
    public void setUp() {
    }

    @Test
    public void varTest() throws IOException {
        String str = "./var";
//        int index = 0;
//        while (str.charAt(0) == '.') {
//            index++;
//            str = str.substring(index);
//        }
//        if (index > 0 && str.charAt(0) == File.separatorChar) str = str.substring(1);
        Path appDir = Paths.get(this.getClass().getResource("/schema_template.yaml").getPath()).getParent();
        Path d1 = Paths.get(str);
        Path d2 = Paths.get("/var/test");
        System.out.println(appDir.resolve(d1));
        System.out.println(appDir.resolve(d2));
        Files.write(appDir.resolve(d1).resolve("test.txt"), "test".getBytes(StandardCharsets.UTF_8));
    }

    @After
    public void tearDown() {
    }
}