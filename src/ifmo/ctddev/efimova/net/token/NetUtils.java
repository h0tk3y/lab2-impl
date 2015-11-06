package ifmo.ctddev.efimova.net.token;

public class NetUtils {
    public static String macAddrFromBytes(byte[] macBytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Constants.MAC_LEN; i++) {
            sb.append(String.format("%02X%s", macBytes[i], (i < macBytes.length - 1) ? "-" : ""));
        }

        return sb.toString();
    }
}
