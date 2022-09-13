package com.raphtory.internals.serialisers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


// https://stackoverflow.com/questions/19862866/find-java-class-dependencies-at-runtime
public class DependencyFinder {
    public static Set<Class<?>> getDependencies(Class<?> from)
            throws IOException, ClassNotFoundException {

        while (from.isArray()) from = from.getComponentType();
        if (from.isPrimitive()) return Collections.emptySet();
        byte[] buf = null;
        int read = 0;
        try (InputStream is = from.getResourceAsStream('/' + from.getName().replace('.', '/') + ".class")) {
            for (int r; ; read += r) {
                int num = Math.max(is.available() + 100, 100);
                if (buf == null) buf = new byte[num];
                else if (buf.length - read < num)
                    System.arraycopy(buf, 0, buf = new byte[read + num], 0, read);
                r = is.read(buf, read, buf.length - read);
                if (r <= 0) break;
            }
        }
        Set<String> names = getDependencies(ByteBuffer.wrap(buf));
        Set<Class<?>> classes = new HashSet<>(names.size());
        ClassLoader cl = from.getClassLoader();
        for (String name : names) classes.add(Class.forName(name, false, cl));
        classes.remove(from);// remove self-reference
        return classes;
    }

    public static Set<String> getDependencies(ByteBuffer bb) {

        if (bb.getInt() != 0xcafebabe)
            throw new IllegalArgumentException("Not a class file");
        bb.position(8);
        final int numC = bb.getChar();
        BitSet clazz = new BitSet(numC), sign = new BitSet(numC);
        for (int c = 1; c < numC; c++) {
            switch (bb.get()) {
                case CONSTANT_Utf8:
                    bb.position(bb.getChar() + bb.position());
                    break;
                case CONSTANT_Integer:
                case CONSTANT_Float:
                case CONSTANT_FieldRef:
                case CONSTANT_MethodRef:
                case CONSTANT_InterfaceMethodRef:
                case CONSTANT_InvokeDynamic:
                    bb.position(bb.position() + 4);
                    break;
                case CONSTANT_Long:
                case CONSTANT_Double:
                    bb.position(bb.position() + 8);
                    c++;
                    break;
                case CONSTANT_String:
                    bb.position(bb.position() + 2);
                    break;
                case CONSTANT_NameAndType:
                    bb.position(bb.position() + 2);// skip name, fall through:
                case CONSTANT_MethodType:
                    sign.set(bb.getChar());
                    break;
                case CONSTANT_Class:
                    clazz.set(bb.getChar());
                    break;
                case CONSTANT_MethodHandle:
                    bb.position(bb.position() + 3);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "constant pool item type " + (bb.get(bb.position() - 1) & 0xff));
            }
        }
        bb.position(bb.position() + 6);
        bb.position(bb.getChar() * 2 + bb.position());
        for (int type = 0; type < 2; type++) { // fields and methods
            int numMember = bb.getChar();
            for (int member = 0; member < numMember; member++) {
                bb.position(bb.position() + 4);
                sign.set(bb.getChar());
                int numAttr = bb.getChar();
                for (int attr = 0; attr < numAttr; attr++) {
                    bb.position(bb.position() + 2);
                    bb.position(bb.getInt() + bb.position());
                }
            }
        }
        bb.position(10);
        HashSet<String> names = new HashSet<>();
        for (int c = 1; c < numC; c++) {
            switch (bb.get()) {
                case CONSTANT_Utf8:
                    int strSize = bb.getChar(), strStart = bb.position();
                    boolean s = sign.get(c);
                    if (clazz.get(c))
                        if (bb.get(bb.position()) == '[') s = true;
                        else addName(names, bb, strStart, strSize);
                    if (s) addNames(names, bb, strStart, strSize);
                    bb.position(strStart + strSize);
                    break;
                case CONSTANT_Integer:
                case CONSTANT_Float:
                case CONSTANT_FieldRef:
                case CONSTANT_MethodRef:
                case CONSTANT_InterfaceMethodRef:
                case CONSTANT_NameAndType:
                case CONSTANT_InvokeDynamic:
                    bb.position(bb.position() + 4);
                    break;
                case CONSTANT_Long:
                case CONSTANT_Double:
                    bb.position(bb.position() + 8);
                    c++;
                    break;
                case CONSTANT_String:
                case CONSTANT_Class:
                case CONSTANT_MethodType:
                    bb.position(bb.position() + 2);
                    break;
                case CONSTANT_MethodHandle:
                    bb.position(bb.position() + 3);
                    break;
                default:
                    throw new AssertionError();
            }
        }
        return names;
    }

    private static void addName(HashSet<String> names,
                                ByteBuffer src, int s, int strSize) {
        final int e = s + strSize;
        StringBuilder dst = new StringBuilder(strSize);
        ascii:
        {
            for (; s < e; s++) {
                byte b = src.get(s);
                if (b < 0) break ascii;
                dst.append((char) (b == '/' ? '.' : b));
            }
            names.add(dst.toString());
            return;
        }
        final int oldLimit = src.limit(), oldPos = dst.length();
        src.limit(e).position(s);
        dst.append(StandardCharsets.UTF_8.decode(src));
        src.limit(oldLimit);
        for (int pos = oldPos, len = dst.length(); pos < len; pos++)
            if (dst.charAt(pos) == '/') dst.setCharAt(pos, '.');
        names.add(dst.toString());
        return;
    }

    private static void addNames(HashSet<String> names,
                                 ByteBuffer bb, int s, int l) {
        final int e = s + l;
        for (; s < e; s++) {
            if (bb.get(s) == 'L') {
                int p = s + 1;
                while (bb.get(p) != ';') p++;
                addName(names, bb, s + 1, p - s - 1);
                s = p;
            }
        }
    }

    private static final byte CONSTANT_Utf8 = 1, CONSTANT_Integer = 3,
            CONSTANT_Float = 4, CONSTANT_Long = 5, CONSTANT_Double = 6,
            CONSTANT_Class = 7, CONSTANT_String = 8, CONSTANT_FieldRef = 9,
            CONSTANT_MethodRef = 10, CONSTANT_InterfaceMethodRef = 11,
            CONSTANT_NameAndType = 12, CONSTANT_MethodHandle = 15,
            CONSTANT_MethodType = 16, CONSTANT_InvokeDynamic = 18;

}