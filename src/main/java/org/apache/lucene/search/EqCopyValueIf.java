package org.apache.lucene.search;

import java.io.IOException;

public interface EqCopyValueIf {
    void copyValue(EqComparatorValue v, int doc) throws IOException;
    int getDocBase();
    int compare(EqEntry a, EqEntry b);
    int compare(EqEntry a, int slot);
    int compareBottom(EqEntry e);
}
