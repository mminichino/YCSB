package site.ycsb.db.couchbase3;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;

public class RoundRobinIterator<T> implements Iterable<T> {
  private final List<T> pool;
  private static final Object COORDINATOR = new Object();

  public RoundRobinIterator(List<T> coll) { this.pool = coll; }

  public void set(int c, T e) { pool.set(c, e); }

  @NotNull
  public Iterator<T> iterator() {
    return new Iterator<>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public T next() {
        synchronized (COORDINATOR) {
          T res = pool.get(index);
          index = (index + 1) % pool.size();
          return res;
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    };
  }
}
