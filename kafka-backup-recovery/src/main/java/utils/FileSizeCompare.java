package utils;

import java.io.File;
import java.util.*;

public class FileSizeCompare {
    private int totalCount;

    public File[] sortFile(File[] files) {
        if (files == null) {
            return files;
        }
        Map<File, Integer> map = new LinkedHashMap<>();
        for (File z : files) {
            map.put(z, Integer.valueOf(totalFiles(z, 0)));
        }
        List<Map.Entry<File, Integer>> list = new ArrayList<>();
        list.addAll(map.entrySet());
        ValueComparator vc = new ValueComparator();
        Collections.sort(list, vc);
        File[] resultFileArray = new File[list.size()];
        for (int i = 0; i < list.size(); i++) {
            resultFileArray[i] = (File) ((Map.Entry) list.get(i)).getKey();
        }
        return resultFileArray;
    }

    public int totalFiles(File inputFile, int num) {
        this.totalCount = num;
        if (inputFile.isDirectory()) {
            File[] files = inputFile.listFiles();
            for (File file : files) {
                this.totalCount++;
                if (file.isDirectory()) {
                    totalFiles(file, this.totalCount);
                }
            }
        }
        return this.totalCount;
    }
}
