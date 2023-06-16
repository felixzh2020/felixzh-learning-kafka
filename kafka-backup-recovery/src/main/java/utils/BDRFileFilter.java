package utils;

import java.io.File;
import java.io.FileFilter;
import java.util.Locale;

public class BDRFileFilter implements FileFilter {
    private final String suffix;

    public BDRFileFilter(String inputDir) {
        this.suffix = inputDir;
    }

    public boolean accept(File pathname) {
        return !pathname.getName().toLowerCase(Locale.getDefault()).endsWith(this.suffix);
    }
}
