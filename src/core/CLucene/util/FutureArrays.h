#pragma once
#include "CLucene/StdHeader.h"

#include <memory>
#include <stdexcept>
#include <vector>

CL_NS_DEF(util)


/**
 * Additional methods from Java 9's <a
 * href="https://docs.oracle.com/javase/9/docs/api/java/util/Arrays.html">
 * {@code java.util.Arrays}</a>.
 * <p>
 * This class will be removed when Java 9 is minimum requirement.
 * Currently any bytecode is patched to use the Java 9 native
 * classes through MR-JAR (Multi-Release JAR) mechanism.
 * In Java 8 it will use THIS implementation.
 * Because of patching, inside the Java source files we always
 * refer to the Lucene implementations, but the final Lucene
 * JAR files will use the native Java 9 class names when executed
 * with Java 9.
 * @lucene.internal
 */
class FutureArrays final : public std::enable_shared_from_this<FutureArrays> {
private:
    FutureArrays();

    // methods in Arrays are defined stupid: they cannot use
    // Objects.checkFromToIndex they throw IAE (vs IOOBE) in the case of fromIndex
    // > toIndex. so this method works just like checkFromToIndex, but with that
    // stupidity added.
    static void CheckFromToIndex(int fromIndex, int toIndex, int length);

    // byte[]

    /**
   * Behaves like Java 9's Arrays.Mismatch
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#mismatch-byte:A-int-int-byte:A-int-int-">Arrays.Mismatch</a>
   */
public:
    static int Mismatch(std::vector<uint8_t> &a, int aFromIndex, int aToIndex,
                        std::vector<uint8_t> &b, int bFromIndex, int bToIndex);

    /**
   * Behaves like Java 9's Arrays.compareUnsigned
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compareUnsigned-byte:A-int-int-byte:A-int-int-">Arrays.compareUnsigned</a>
   */
    static int CompareUnsigned(const uint8_t *a, int aFromIndex,
                               int aToIndex, const uint8_t *b,
                               int bFromIndex, int bToIndex);
    static int CompareUnsigned(std::vector<uint8_t> &a, int aFromIndex, int aToIndex,
                               std::vector<uint8_t> &b, int bFromIndex,
                               int bToIndex);

    static int CompareNumeric(const uint8_t *a,
                              int aLen,
                              const uint8_t *b,
                              int bLen);

    /**
   * Behaves like Java 9's Arrays.equals
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-byte:A-int-int-byte:A-int-int-">Arrays.equals</a>
   */
    static bool Equals(std::vector<uint8_t> &a, int aFromIndex, int aToIndex,
                       std::vector<uint8_t> &b, int bFromIndex, int bToIndex);

    // uint8_t[]

    /**
   * Behaves like Java 9's Arrays.Mismatch
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#mismatch-uint8_t:A-int-int-uint8_t:A-int-int-">Arrays.Mismatch</a>
   */
    static int Mismatch(std::vector<wchar_t> &a, int aFromIndex, int aToIndex,
                        std::vector<wchar_t> &b, int bFromIndex, int bToIndex);

    /**
   * Behaves like Java 9's Arrays.compare
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compare-uint8_t:A-int-int-uint8_t:A-int-int-">Arrays.compare</a>
   */
    static int Compare(std::vector<wchar_t> &a, int aFromIndex, int aToIndex,
                       std::vector<wchar_t> &b, int bFromIndex, int bToIndex);

    /**
   * Behaves like Java 9's Arrays.equals
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-uint8_t:A-int-int-uint8_t:A-int-int-">Arrays.equals</a>
   */
    static bool Equals(std::vector<wchar_t> &a, int aFromIndex, int aToIndex,
                       std::vector<wchar_t> &b, int bFromIndex, int bToIndex);

    // int[]

    /**
   * Behaves like Java 9's Arrays.compare
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compare-int:A-int-int-int:A-int-int-">Arrays.compare</a>
   */
    static int Compare(std::vector<int> &a, int aFromIndex, int aToIndex,
                       std::vector<int> &b, int bFromIndex, int bToIndex);

    /**
   * Behaves like Java 9's Arrays.equals
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-int:A-int-int-int:A-int-int-">Arrays.equals</a>
   */
    static bool Equals(std::vector<int> &a, int aFromIndex, int aToIndex,
                       std::vector<int> &b, int bFromIndex, int bToIndex);

    // long[]

    /**
   * Behaves like Java 9's Arrays.compare
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compare-long:A-int-int-long:A-int-int-">Arrays.compare</a>
   */
    static int Compare(std::vector<int64_t> &a, int aFromIndex, int aToIndex,
                       std::vector<int64_t> &b, int bFromIndex, int bToIndex);

    /**
   * Behaves like Java 9's Arrays.equals
   * @see <a
   * href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-long:A-int-int-long:A-int-int-">Arrays.equals</a>
   */
    static bool Equals(std::vector<int64_t> &a, int aFromIndex, int aToIndex,
                       std::vector<int64_t> &b, int bFromIndex, int bToIndex);
};
CL_NS_END
