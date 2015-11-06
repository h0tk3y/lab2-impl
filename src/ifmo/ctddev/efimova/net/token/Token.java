package ifmo.ctddev.efimova.net.token;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class Token {

    public int version;
    public String macAddr;
    public byte[] macBytes;
    public int nDigits;
    byte digits[];

    public Token(int version, byte[] macBytes, int nDigits, List<Byte> bytes) {
        this.version  = version;
        this.macBytes = macBytes;
        this.nDigits  = nDigits;
        digits = new byte[bytes.size() + Constants.N_DIGITS];
        for (int i = 0; i < bytes.size(); i++) {
            digits[i] = bytes.get(i);
        }
        macAddr = NetUtils.macAddrFromBytes(macBytes);
    }

    public Token(DataInputStream is) throws IOException {
        byte m = is.readByte();
        macBytes = new byte[6];
        is.read(macBytes);
        macAddr = NetUtils.macAddrFromBytes(macBytes);
        version = is.readInt();
        nDigits = is.readInt();
        digits = new byte[nDigits + Constants.N_DIGITS];
        is.read(digits, 0, nDigits);
    }

    public Token(byte arr[]) {
        macBytes = Arrays.copyOfRange(arr, 1, Constants.MAC_LEN + 1);
        macAddr = NetUtils.macAddrFromBytes(macBytes);
        int len = Constants.MAC_LEN + 1;
        ByteBuffer bbuf = ByteBuffer.wrap(arr, len, 4);
        version = bbuf.getInt();
        len += 4;
        bbuf = ByteBuffer.wrap(arr, len, 4);
        nDigits = bbuf.getInt();
        len += 4;
        digits = Arrays.copyOfRange(arr, len, len + nDigits);
    }

    public byte[] toBytes() {
        byte res[] = new byte[1 + Constants.MAC_LEN + 4 * 3 + nDigits];
        res[0] = 0x03;
        System.arraycopy(macBytes, 0, res, 1, Constants.MAC_LEN);
        byte[] intbytes = ByteBuffer.allocate(4).putInt(version).array();
        int len =  1 + Constants.MAC_LEN;
        System.arraycopy(intbytes, 0, res, len, 4);
        intbytes = ByteBuffer.allocate(4).putInt(nDigits).array();
        len += 4;
        System.arraycopy(intbytes, 0, res, len, 4);
        len += 4;
        System.arraycopy(digits, 0, res, len, nDigits);
        return res;
    }
}
