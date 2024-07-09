package site.ycsb;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Batch {

  public static <T> Stream<List<T>> split(List<T> source, int length) {
    int size = source.size();
    if (size == 0)
      return Stream.empty();
    int fullChunks = (size - 1) / length;
    return IntStream.range(0, fullChunks + 1).mapToObj(
        n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
  }

}
