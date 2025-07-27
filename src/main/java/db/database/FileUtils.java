package db.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@UtilityClass
public class FileUtils {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // filter out files older than 14 days
    public static Integer findLatestLogFile(String path) {
        File directory = new File(path);

        long cutoff = LocalDateTime.now()
            .minusDays(14)
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli();

        File[] files = directory.listFiles((dir, name) ->
            name.matches(".*_\\d+\\.log") && new File(dir, name).lastModified() > cutoff
        );

        if (files == null || files.length == 0) {
            return 0;
        }

        int max = 0;
        Pattern pattern = Pattern.compile(".*_(\\d+)\\.log");

        for (File file : files) {
            Matcher matcher = pattern.matcher(file.getName());
            if (matcher.matches()) {
                int num = Integer.parseInt(matcher.group(1));
                if (num > max) {
                    max = num;
                }
            }
        }

        return max;
    }

    public static boolean fileExists(String path) {
        return new File(path).exists();
    }

    public static long bytesWrittenSoFar(String path) {
        File file = new File(path);
        if (!file.exists() || !file.isFile()) {
            return 0L;
        }
        return file.length();
    }

    public static String readLastLine(File file) throws IOException {
        if (!file.exists() || !file.isFile()) {
            return null;
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long length = raf.length();
            if (length == 0) return null;

            long pos = length - 1;
            raf.seek(pos);

            // Move backward to find the last newline
            while (pos > 0) {
                int b = raf.read();
                if (b == '\n' || b == '\r') {
                    break;
                }
                raf.seek(--pos);
            }

            String lastLine = raf.readLine();
            return lastLine != null ? new String(lastLine.getBytes(), StandardCharsets.UTF_8) : null;
        }
    }

    public static File findLatestSnapshot(File path) {
        File[] files = path.listFiles((dir, name) -> name.endsWith(".snapshot"));
        if (files == null || files.length == 0) {
            return null;
        }

        return Arrays.stream(files)
            .max(Comparator.comparingLong(file -> {
                String name = file.getName().replace(".snapshot", "");
                try {
                    return Long.parseLong(name);
                } catch (NumberFormatException e) {
                    return Long.MIN_VALUE;
                }
            })).orElse(null);

    }
}
