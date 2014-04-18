package util;

/**
 * Created by kostas on 4/15/14.
 */
public class Pair<C1, C2> {

    public C1 first;

    public C2 second;

    public Pair(C1 first, C2 second){
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (!first.equals(pair.first)) return false;
        if (!second.equals(pair.second)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }
}
