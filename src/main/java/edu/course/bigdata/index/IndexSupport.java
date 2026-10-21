package edu.course.bigdata.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Shared tokenization and deterministic output, independent of Hadoop. */
public final class IndexSupport {
    private static final Pattern WORD = Pattern.compile("[a-z]+");

    private IndexSupport() { }

    public static List<String> words(String line) {
        List<String> words = new ArrayList<String>();
        Matcher matcher = WORD.matcher(line.toLowerCase(Locale.ROOT));
        while (matcher.find()) {
            words.add(matcher.group());
        }
        return words;
    }

    public static String postings(Map<String, Long> counts) {
        StringBuilder result = new StringBuilder();
        long total = 0;
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            result.append('<').append(entry.getKey()).append(',')
                  .append(entry.getValue()).append(">;");
            total = Math.addExact(total, entry.getValue());
        }
        return result.append("<total,").append(total).append(">.").toString();
    }
}
