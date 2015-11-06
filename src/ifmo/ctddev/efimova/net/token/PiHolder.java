package ifmo.ctddev.efimova.net.token;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PiHolder {

    private List<Byte> digits;
    private String piFile = "Pi100million.txt";

    private static class LazyHolder {
        public static final PiHolder INSTANCE = new PiHolder();
    }

    public static PiHolder getInstance() {
        return LazyHolder.INSTANCE;
    }

    public void addNDigits(byte[] pi) {
        int len = digits.size();
        for (int i = pi.length - Constants.N_DIGITS; i < pi.length; i++) {
            pi[i] = digits.get(i % len);
        }
    }

    private PiHolder() {
        digits = new ArrayList<>(10 * 1000 * 1000 + 60);
        BufferedReader br = null;
        String line = "";
        try {
            br = new BufferedReader(new FileReader(piFile));
            while ((line = br.readLine()) != null && digits.size() < 10000000) {

                String s = line.split(":")[0].replaceAll(" ", "");
                for (int i = 0; i < s.length(); i++) {
                    digits.add((byte) (s.charAt(i) - '0'));
                }
            }

        } catch (FileNotFoundException e) {
            System.out.println("file: " + piFile + " not found!");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
