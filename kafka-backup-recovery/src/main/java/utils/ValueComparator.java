package utils;

import java.io.File;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<Map.Entry<File, Integer>>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Map.Entry<File, Integer> m, Map.Entry<File, Integer> n) {
        return n.getValue() - m.getValue();
    }
}
