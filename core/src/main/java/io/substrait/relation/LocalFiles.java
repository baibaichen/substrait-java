package io.substrait.relation;

import org.immutables.value.Value;

@Value.Immutable
public abstract class LocalFiles extends AbstractReadRel {

    public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
        return visitor.visit(this);
    }

    public static ImmutableLocalFiles.Builder builder() {
        return ImmutableLocalFiles.builder();
    }
}
