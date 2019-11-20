package io.hentitydb.entity;

import io.hentitydb.serialization.*;
import io.hentitydb.serialization.Codec;

import javax.persistence.Entity;
import javax.persistence.Enumerated;
import java.lang.reflect.Field;

public class MappingUtils {

    @SuppressWarnings("unchecked")
    static <T> Codec<T> getCodecForField(Field field, boolean isDescending) {
        Codec<T> codec;
        io.hentitydb.entity.Codec codecAnnotation = field.getAnnotation(io.hentitydb.entity.Codec.class);
        Enumerated enumAnnotation = field.getAnnotation(Enumerated.class);
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
        } else if (enumAnnotation != null) {
            codec = (Codec<T>) new EnumCodec(field.getType(), enumAnnotation.value());
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
