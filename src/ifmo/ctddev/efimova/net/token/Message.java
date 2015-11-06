package ifmo.ctddev.efimova.net.token;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Message {
    public int version;
    private byte[] macBytes;
    public String macAddr;

    public Message(int version, byte[] macBytes) {
        this.version  = version;
        this.macBytes = macBytes;
        this.macAddr  = NetUtils.macAddrFromBytes(macBytes);
    }

    public Message(byte arr[]) {
        this(
            ByteBuffer.wrap(arr, Constants.MAC_LEN + 1, 4).getInt(),
            Arrays.copyOfRange(arr, 1, Constants.MAC_LEN + 1)
        );
    }

    public byte[] toBytes() {
        byte res[] = new byte[1 + Constants.MAC_LEN + 4];
        res[0] = 0x02;

        System.arraycopy(macBytes, 0, res, 1, Constants.MAC_LEN);

        byte[] versionBytes = ByteBuffer.allocate(4).putInt(version).array();
        System.arraycopy(versionBytes, 0, res, 1 + Constants.MAC_LEN, 4);

        return res;
    }
}
