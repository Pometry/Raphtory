/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import org.apache.arrow.vector.holders.NullableVarCharHolder;

/**
 * A set of string related utility functions.
 *<p>
 * As string values are expected to be used frequently then
 * we've made use of thread-locals for some of the more
 * commonly used items such as byte/char arrays and
 * Arrow's NullableVarCharHolder.
 */
public class RaphtoryStringUtils {
    private static final ThreadLocal<NullableVarCharHolder> _vcHolderTL = ThreadLocal.withInitial(NullableVarCharHolder::new);
    private static final ThreadLocal<StringBuilder> _tmpSBTL = ThreadLocal.withInitial(StringBuilder::new);
    private static final ThreadLocal<byte[]> _tmpBytesTL = ThreadLocal.withInitial(() -> new byte[1024]);
    private static final ThreadLocal<char[]> _tmpCharsTL = ThreadLocal.withInitial(() -> new char[1024]);


    /**
     * This function returns a thread-local NullableVarCharHolder.
     * The returned instance should be used immediately in a short block
     * to prevent the chance of another method trying to access the
     * same instance.
     *
     * @return the instance to use
     */
    public static NullableVarCharHolder getTmpVarCharHolder() {
        return _vcHolderTL.get();
    }


    /**
     * This function returns a thread-local StringBuilder.
     * The returned instance should be used immediately in a short block
     * to prevent the chance of another method trying to access the
     * same instance.
     *
     * @return the instance to use
     */
    public static StringBuilder getTmpStringBuilder() {
        return _tmpSBTL.get();
    }


    /**
     * This function copies the chars in the StringBuilder
     * into a temporary byte array (converting from chars to utf8)
     * and then sets up the supplied NullableVarCharHolder so that
     * the resultant bytes can be copied into an Arrow VarCharVector.
     *<p>
     * As thread-locals are used here, the returned bytes and holder
     * should be used immediately in a short block to prevent the
     * chance of another method trying to access the same instances.
     *
     * @param sb the string to use
     * @param vcHolder the holder to configure
     *
     * @return the temporary byte array
     */
    public static byte[] copy(StringBuilder sb, NullableVarCharHolder vcHolder) {
        int n = sb.length();

        byte[] tmpBytes = _tmpBytesTL.get();
        char[] tmpChars = _tmpCharsTL.get();

        if (n > tmpChars.length) {
            tmpChars = new char[n+16];
            _tmpCharsTL.set(tmpChars);
        }
        sb.getChars(0, n, tmpChars, 0);
        int utf8Len = getUTF8Length(tmpChars, 0, n);

        if (utf8Len > tmpBytes.length) {
            tmpBytes = new byte[utf8Len+16];
            _tmpBytesTL.set(tmpBytes);
        }

        copyUTF8(tmpChars, 0, n, tmpBytes, 0);

        vcHolder.start = 0;
        vcHolder.end = utf8Len;
        vcHolder.buffer = null;
        vcHolder.isSet = 1;

        return tmpBytes;
    }


    /**
     * This function copies the bytes in an Arrow VarCharVector
     * into the supplied StringBuilder, converting them to 16-bit
     * chars as required.
     *
     * @param holder the holder that describes where the source bytes are
     * @param sb the string to update
     */
    public static void copy(NullableVarCharHolder holder, StringBuilder sb) {
        sb.setLength(0);

        if (holder.isSet==0) {
            return;
        }

        int nBytes = holder.end - holder.start;
        byte[] tmpBytes = _tmpBytesTL.get();
        if (nBytes > tmpBytes.length) {
            tmpBytes = new byte[nBytes+16];
            _tmpBytesTL.set(tmpBytes);
        }
        holder.buffer.getBytes((long)holder.start, tmpBytes, 0, nBytes);

        int nChars = getUTF8CharLength(tmpBytes, 0, nBytes);
        char[] tmpChars = _tmpCharsTL.get();
        if (nChars > tmpChars.length) {
            tmpChars = new char[nChars+16];
            _tmpCharsTL.set(tmpChars);
        }
        utf8ToChars(tmpBytes, 0, nBytes, tmpChars);

        sb.append(tmpChars, 0, nChars);
    }


    /**
     * Returns the number of utf8 bytes required to encode this
     * sequence of chars.
     *
     * @param chars the chars to check
     * @param offset offset into the array
     * @param length number of items to check
     *
     * @return the number of utf8 encoded bytes required
     */
    public static int getUTF8Length(char[] chars, int offset, int length) {
        int utflen = length;
        for (int i=0; i<length; ++i) {
            char c = chars[i+offset];
            if (c <= 0x007F) {
                continue;
            }
            else if (c <= 0x07FF) {
                utflen++;
            }
            else {
                utflen += 2;
            }
        }

        return utflen;
    }


    /**
     * Copies the specified chars into a byte-array, converting to utf-8
     * as it goes along.
     *
     * @param chars the char array to use
     * @param offset the initial offset in the char array
     * @param count the number of chars to process
     * @param bytes the output byte array
     * @param bIndex the starting output position in the byte array
     *
     * @return the ending output position in the byte array
     */
    public static int copyUTF8(char[] chars, int offset, int count, byte[] bytes, int bIndex) {
        for (int i=0; i<count; ++i) {
            char c = chars[i+offset];
            if (c<=0x007F) {
                bytes[bIndex++] = (byte)c;
            }
            else if (c<=0x07FF) {
                bytes[bIndex++] = (byte)(0xC0 | ((c >>> 6) & 0x1F));
                bytes[bIndex++] = (byte)(0x80 | ((c >>> 0) & 0x3F));
            }
            else {
                bytes[bIndex++] = (byte)(0xE0 | ((c >>> 12) & 0x0F));
                bytes[bIndex++] = (byte)(0x80 | ((c >>>  6) & 0x3F));
                bytes[bIndex++] = (byte)(0x80 | ((c >>>  0) & 0x3F));
            }
        }

        return bIndex;
    }


    /**
     * Returns the number of 16-bit chars required to represent this
     * utf8 encoded byte string.
     *<p>
     * This function does not attempt to strongly validate the
     * utf8 encoded byte string.
     *
     * @param bs the byte array to process
     * @param offset the starting offset into the byte array
     * @param length the number of bytes to process
     *
     * @return the number of chars required to represent the string
     */
    public static int getUTF8CharLength(byte[] bs, int offset, int length) {
        int n=0;
        for (int i=offset; i<offset+length; ++i) {
            byte b = bs[i];
            if (b>0) {
                ++n;
                continue;
            }

            int mode = (b & 0xE0);
            if (mode==0xC0) {
                ++n;
                ++i;
            }
            else if (mode==0xE0) {
                ++n;
                i += 2;
            }
            else {
                throw new IllegalArgumentException("Invalid utf8 sequence: " + Integer.toHexString(mode));
            }
        }

        return n;
    }


    /**
     * Copies the utf-8 encoded bytes into the specified char
     * array as 16-bit chars.
     *<p>
     * This function does not attempt to strongly validate the
     * utf8 encoded byte string.
     *
     * @param bs the byte array to process
     * @param offset the starting offset into the byte array
     * @param length the number of bytes to process
     * @param cs the char array to output into
     *
     * @return the number of chars copied
     */
    public static int utf8ToChars(byte[] bs, int offset, int length, char[] cs) {
        int n=0;
        for (int i=offset; i<offset+length; ++i) {
            byte b = bs[i];
            if (b > 0) {
                cs[n++] = (char)b;
                continue;
            }

            int mode = b & 0xE0;
            if (mode==0xC0) {
                byte b2 = bs[++i];
                if ((b2 & 0xC0) != 0x80) {
                    throw new IllegalArgumentException("UTF Format");
                }
                cs[n++] = (char) (((b & 0x1F) << 6) | (b2 & 0x3F));
            }
            else if (mode == 0xE0) {
                byte b2 = bs[++i];
                byte b3 = bs[++i];

                if (((b2 & 0xC0) != 0x80) || ((b3 & 0xC0) != 0x80)) {
                    throw new IllegalArgumentException("UTF Format");
                }

                cs[n++] = (char)(((b & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));
            }
            else {
                throw new IllegalArgumentException("UTF Format");
            }
        }

        return n;
    }
}