package bean;

import java.util.Objects;

/**
 * element may be null
 *
 * @param <L> left type
 * @param <M> middle type
 * @param <R> right type
 */
public class Triple<L, M, R> {


    private final L left;
    private final M middle;
    private final R right;

    public static <L, M, R> Triple<L, M, R> of(final L left, final M middle, final R right) {
        return new Triple<>(left, middle, right);
    }

    private Triple(final L left, final M middle, final R right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }


    public L getLeft() {
        return left;
    }

    public M getMiddle() {
        return middle;
    }

    public R getRight() {
        return right;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Triple<?, ?, ?>) {
            final Triple<?, ?, ?> other = (Triple<?, ?, ?>) obj;
            return Objects.equals(getLeft(), other.getLeft())
                    && Objects.equals(getMiddle(), other.getMiddle())
                    && Objects.equals(getRight(), other.getRight());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (getLeft() == null ? 0 : getLeft().hashCode()) ^
                (getMiddle() == null ? 0 : getMiddle().hashCode()) ^
                (getRight() == null ? 0 : getRight().hashCode());
    }

    @Override
    public String toString() {
        return "(" + getLeft() + "," + getMiddle() + "," + getRight() + ")";
    }

}
