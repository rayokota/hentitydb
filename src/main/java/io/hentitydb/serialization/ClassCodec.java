package io.hentitydb.serialization;

import com.google.common.base.Throwables;

/**
 * A {@link Codec} implementation which stores Class names.
 */
public class ClassCodec extends AbstractCodec<Class> {

    private ClassLoader classLoader;

    public ClassCodec() {
    }

    public ClassCodec(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public void encode(Class value, WriteBuffer buffer) {
        buffer.writeUtf8String(value.getName());
    }

    @Override
    public Class decode(ReadBuffer buffer) {
        String className = buffer.readUtf8String();
        try {
            return classLoader != null ? Class.forName(className, true, classLoader) : Class.forName(className);
        } catch (ClassNotFoundException cnfe) {
            throw Throwables.propagate(cnfe);
        }
    }
}
