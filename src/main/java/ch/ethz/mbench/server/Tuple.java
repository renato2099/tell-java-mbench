package ch.ethz.mbench.server;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Tuples
 */
public class Tuple {
    private Object fieldValues[];
    private String fieldNames[];
    private int numFields;

    private static String mSyllables[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    public Tuple(int n) {
        numFields = n;
        fieldValues = new Object[n];
        fieldNames = new String[n];
    }

    public static Tuple createInsert(int nColumns) {
        Tuple nTuple = new Tuple(nColumns);
        for (int i = 0; i < nColumns; i++) {
            nTuple.fieldNames[i] = "A" + (i % 10);
            switch (i) {
                case 0:
                    nTuple.fieldValues[i] = getRandomDouble(0.0, 1.0);
                    break;
                case 1:
                    nTuple.fieldValues[i] = getRandomInt();
                    break;
                case 2:
                    nTuple.fieldValues[i] = getRandomInt(0, 10000);
                    break;
                case 3:
                    nTuple.fieldValues[i] = getRandomShort(0, 2);
                    break;
                case 4:
                    nTuple.fieldValues[i] = getRandomShort(0, 256);
                    break;
                case 5:
                    nTuple.fieldValues[i] = getRandomLong(Long.MIN_VALUE, 0);
                    break;
                case 6:
                    nTuple.fieldValues[i] = getRandomLong(Long.MIN_VALUE, Long.MAX_VALUE);
                    break;
                case 7:
                    nTuple.fieldValues[i] = getRandomDouble(Double.MIN_VALUE, Double.MAX_VALUE);
                    break;
                case 8:
                    nTuple.fieldValues[i] = getRandomString(2);
                    break;
                case 9:
                    nTuple.fieldValues[i] = getRandomString(3);
                    break;
                default:
                    throw new RuntimeException("Number of colums not supported!");
            }

        }
        return nTuple;
    }

    public static double getRandomDouble(Double... bounds) {
        double nextDouble = ThreadLocalRandom.current().nextDouble();
        if (bounds != null & bounds.length == 2)
            nextDouble = ThreadLocalRandom.current().nextDouble(bounds[0], bounds[1]);
        return nextDouble;
    }

    public static int getRandomInt(Integer... bounds) {
        int nextInt = ThreadLocalRandom.current().nextInt();
        if (bounds != null & bounds.length == 2)
            nextInt = ThreadLocalRandom.current().nextInt(bounds[0], bounds[1]);
        return nextInt;
    }

    public static long getRandomLong(long lb, long hb) {
        return ThreadLocalRandom.current().nextLong(lb, hb);
    }

    public static int getRandomShort(int lb, int hb) {
        if (hb > Short.MAX_VALUE) hb = Short.MAX_VALUE;
        return ThreadLocalRandom.current().nextInt(lb, hb);
    }

    public static String getRandomString(int nSyllable) {
        StringBuilder sb = new StringBuilder();
        while (nSyllable > 0) {
            int sy = ThreadLocalRandom.current().nextInt(0, 10);
            sb.append(mSyllables[sy]);
            nSyllable--;
        }
        return sb.toString();
    }

    public void setField(int pos, Object obj) {
        if (pos < fieldValues.length)
            fieldValues[pos] = obj;
        else
            throw new RuntimeException("Invalid position for record field");
    }

    public int getNumFields() {
        return numFields;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public Object[] getFieldValues() {
        return fieldValues;
    }

    public static long rndKey(long baseInsKey, long baseDelKey, long nClients, long clientId) {
        long k = getRandomLong(baseDelKey, baseInsKey);
        k = (k / nClients) * nClients;
        k += clientId;
        if (k >= baseInsKey) {
            k -= nClients;
        } else if (k < baseDelKey) {
            k += nClients;
        }
        return k;
    }

    public static Tuple rndUpdate(int totCols) {
        Tuple updTuple = new Tuple(totCols);
        int offset = totCols == 10 ? 0 : getRandomInt(0, totCols / 10 - 1);
        // set names
        for (int i = 0; i < totCols; i++)
            updTuple.fieldNames[i] = "A" + (i % 10);
        // double columns
        if (getRandomInt(0, 10) <= 5)
            updTuple.setField(offset, getRandomDouble());
        else
            updTuple.setField(offset + 7, getRandomDouble());
        // int columns
        if (getRandomInt(0, 10) <= 5)
            updTuple.setField(offset + 1, getRandomInt());
        else
            updTuple.setField(offset + 2, getRandomInt(0, 10000));
        // shorts
        if (getRandomInt(0, 10) <= 5)
            updTuple.setField(offset + 3, getRandomShort(0, 1));
        else
            updTuple.setField(offset + 4, getRandomInt(0, 255));
        // longs
        if (getRandomInt(0, 10) <= 5)
            updTuple.setField(offset + 5, getRandomLong(Long.MIN_VALUE, 0));
        else
            updTuple.setField(offset + 6, getRandomLong(Long.MIN_VALUE, Long.MAX_VALUE));
        // String
        if (getRandomInt(0, 10) <= 5)
            updTuple.setField(offset + 8, getRandomString(2));
        else
            updTuple.setField(offset + 9, getRandomString(3));
        return updTuple;
    }
}
