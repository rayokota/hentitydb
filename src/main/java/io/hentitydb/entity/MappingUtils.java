package io.hentitydb.entity;

import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.Codecs;
import io.hentitydb.serialization.SaltingCodec;

import javax.persistence.Entity;
import java.lang.reflect.Field;

public class MappingUtils {

    @SuppressWarnings("unchecked")
    static <T> Codec<T> getCodecForField(Field field, boolean isDescending) {
        Codec<T> codec;
        io.hentitydb.entity.Codec codecAnnotation = field.getAnnotation(io.hentitydb.entity.Codec.class);
        if (codecAnnotation != null) {
            final Class<?> codecClass = codecAnnotation.value();
            if (!Codec.class.isAssignableFrom(codecClass)) {
                throw new RuntimeException("Value for @Codec annotation is not a subclass of " + Codec.class.getName());
            }
            try {
                codec = (Codec<T>) codecClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to invoke default ctor of " + codecClass.getName());
            }
        } else {
            codec = Codecs.getCodec(field.getType(), isDescending);
        }
        Salt saltAnnotation = field.getAnnotation(Salt.class);
        if (saltAnnotation != null) {
            codec = new SaltingCodec<>(codec);
        }
        return codec;
    }

    static String getEntityName(Entity entityAnnotation, Class<?> clazz) {
        final String name = entityAnnotation.name();
        if (name == null || name.isEmpty()) {
            String className = clazz.getName();
            int index = className.lastIndexOf(".");
            return className.substring(index + 1).toLowerCase();
        } else {
            return name;
        }
    }
}
