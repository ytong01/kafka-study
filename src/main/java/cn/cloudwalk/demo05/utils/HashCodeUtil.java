package cn.cloudwalk.demo05.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashCodeUtil {

    public static String md5(byte[] bytes) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("md5");
        digest.update(bytes);
        byte[] data = digest.digest();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < data.length; i++) {

            sb.append(Integer.toString((data[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    public static String getHashString(Object... objects) throws NoSuchAlgorithmException {

        StringBuffer sb = new StringBuffer();
        for (Object obj : objects) {
            sb.append(obj == null ? "" : obj);
            sb.append("-");
        }
        return md5(sb.toString().getBytes());
    }
}
