package ifmo.ctddev.efimova.net.token;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Node {
    private byte[] macBytes;
    private String macAddr;
    private DatagramSocket serverSocket;
    private final Map<String, String> macIP;
    private List<Byte> piBackup;

    private volatile NavigableSet<String> neighbours;
    private volatile int version;
    private volatile State state;
    private volatile String nextInRing;
    private volatile String prevInRing;

    private volatile boolean needsStopReconfigure;

    public Node() {
        this.macIP = new TreeMap<>();
        this.version = 0;
        this.state = State.CONFIG;
        this.piBackup = new ArrayList<>();
    }

    private void setThisNodeMacBytesAndAddr() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;

            getIP:
            while (interfaces.hasMoreElements()) {
                NetworkInterface element = interfaces.nextElement();
                if (element.isLoopback() || !element.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = element.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress i = addresses.nextElement();
                    if (i instanceof Inet4Address) {
                        ip = i;
                        System.out.println("Current IP address : " + ip.getHostAddress());
                        if (element.toString().startsWith("name:wlan0")) {
                            break getIP;
                        }
                    }
                }
            }

            macBytes = NetworkInterface.getByInetAddress(ip).getHardwareAddress();
        } catch (SocketException e) {
            e.printStackTrace();
        }

        this.macAddr = NetUtils.macAddrFromBytes(macBytes);
        System.out.println("Current MAC address : " + macAddr);
    }

    public void run() {
        setThisNodeMacBytesAndAddr();
        setServerSocket();

        new Thread(this::receiveReconfigure).start();
        new Thread(this::receiveToken).start();
        new Thread(() -> {
            try {
                Thread.sleep(Constants.TICK * 10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("R!! Planned reconfiguration");
            initReconfigure(version + 1);
        }).start();
        initReconfigure(version + 1);
    }

    private void setServerSocket() {
        try {
            serverSocket = new DatagramSocket(Constants.BROADCAST_PORT);
            serverSocket.setBroadcast(true);
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    private void receiveReconfigure() {
        while (true) {
            try {
                System.out.println("WAITING FOR R");
                byte[] receiveData = new byte[Constants.CONF_MSG_SIZE];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);

                String ip = receivePacket.getAddress().getHostAddress();
                Message message = new Message(receiveData);
                System.out.println("-> RR "
                                   + "myversion = " + this.version
                                   + ", version = " + message.version
                                   + ", MAC = " + message.macAddr
                                   + ", IP = " + ip);

                if (message.version > this.version) {
                    macIP.put(message.macAddr, ip);
                    reconfigure(message.version, message.macAddr);
                } else if ((message.version < this.version) || (message.version == this.version) && (state != State.CONFIG)) {
                    reconfigure(Math.max(this.version, message.version) + 1, this.macAddr);
                } else {
                    macIP.put(message.macAddr, ip);
                }
            } catch (IOException e) {
                serverSocket.close();
                setServerSocket();
            }
        }
    }

    private void reconfigure(int newVersion, String initializerMacAddr) {
        System.out.println("CONFIG v" + newVersion + ", MAC = " + initializerMacAddr);

        this.state = State.CONFIG;
        this.version = newVersion;
        this.neighbours = new TreeSet<>(Arrays.asList(this.macAddr, initializerMacAddr));
        this.needsStopReconfigure = false;

        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < Constants.BROADCASTS_COUNT; ++i) {
                sendReconfigure(this.macBytes, version);
                try {
                    Thread.sleep(Constants.TICK * 1000 / Constants.BROADCASTS_COUNT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Runnable thread2 = () -> {
            try {
                while (!needsStopReconfigure && state == State.CONFIG) {
                    byte[] receiveData = new byte[Constants.CONF_MSG_SIZE];
                    DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                    serverSocket.receive(receivePacket);
                    if (needsStopReconfigure) {
                        break;
                    }
                    String ip = receivePacket.getAddress().getHostAddress();
                    Message message = new Message(receiveData);

                    if (message.macAddr.equals(this.macAddr)) {
                        continue;
                    }

                    System.out.println("-> R receive reconfigure: "
                                       + "myversion = " + this.version
                                       + ", version = " + message.version
                                       + ", MAC = " + message.macAddr
                                       + ", IP = " + ip);
                    if (message.version >= this.version) {
                        neighbours.add(message.macAddr);
                        macIP.put(message.macAddr, ip);
                        updateNextAndPrev();
                    }

                    if (message.version > this.version) {
                        System.out.println(">>> adopted version " + message.version);
                        this.version = message.version;
                        new Thread(() -> sendReconfigure(this.macBytes, version));
                    } else if (message.version < this.version) {
                        new Thread(() -> sendReconfigure(this.macBytes, version));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };

        thread1.start();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(thread2).get(Constants.TICK, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            needsStopReconfigure = true;
        }

        executor.shutdownNow();

        try {
            thread1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Neighbours: " + neighbours.size());

        if (neighbours.size() <= 1) {
            System.out.println("R!! Alone");
            reconfigure(version + 1, this.macAddr);
        } else if (state == State.CONFIG) {
            state = State.WORKING;
            System.out.println("WORKING, v" + version + ", prev = " + prevInRing + ", next = " + nextInRing);

            if (neighbours.first().equals(this.macAddr)) {
                new Thread(() -> sendToken(new Token(version, macBytes, piBackup.size(), piBackup))).start();
            }
        }

    }

    private void sendReconfigure(byte[] mac, int newVersion) {
        try {
            InetAddress IPAddress0 = InetAddress.getByName(Constants.BROADCAST_ADDRESS);

            byte data[] = new Message(newVersion, mac).toBytes();
            DatagramPacket sendPacketB = new DatagramPacket(data, data.length, IPAddress0, Constants.BROADCAST_PORT);

            for (int i = 0; i < Constants.BROADCASTS_COUNT; i++) {
                System.out.println("<- R v" + newVersion);
                serverSocket.send(sendPacketB);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateNextAndPrev() {
        nextInRing = neighbours.higher(this.macAddr);
        if (nextInRing == null) {
            nextInRing = neighbours.first();
        }

        prevInRing = neighbours.lower(this.macAddr);
        if (prevInRing == null) {
            prevInRing = neighbours.last();
        }
    }

    private void receiveToken() {
        //noinspection InfiniteLoopStatement
        while (true) {
            try (ServerSocket serverTCPSocket = new ServerSocket(Constants.TCP_PORT)) {
                serverTCPSocket.setSoTimeout(Constants.TICK * 2000);
                try (Socket prevSocket = serverTCPSocket.accept()) {
                    prevSocket.setSoTimeout(Constants.TICK * 2000);
                    DataInputStream is = new DataInputStream(prevSocket.getInputStream());
                    Token token = new Token(is);

                    System.out.println("-> <> receive token: "
                                       + "myversion = " + version
                                       + ", version = " + token.version
                                       + ", MAC = " + token.macAddr
                                       + ", nDigits = " + token.nDigits);

                    if (token.version != this.version || !token.macAddr.equals(prevInRing)) {
                        initReconfigure(Math.max(token.version, this.version) + 1);
                    } else {
                        if (state != State.WORKING) {
                            state = State.WORKING;
                            System.out.println("WORKING, v" + version + ", prev = " + prevInRing + ", next = " + nextInRing);
                        }
                        savePi(token);
                        sendToken(new Token(version, macBytes, piBackup.size(), piBackup));
                    }
                }
            } catch (IOException e) {
                if (state != State.CONFIG) {
                    System.out.println("-> !! receive failed " + e.getMessage());
                    initReconfigure(version + 1);
                }
            }
        }
    }

    private void savePi(Token token) {
        for (int i = piBackup.size(); i < token.digits.length; i++) {
            piBackup.add(token.digits[i]);
        }
    }

    private void sendToken(Token token) {

        try (Socket nextSocket = new Socket()) {
            nextSocket.connect(new InetSocketAddress(macIP.get(nextInRing), Constants.TCP_PORT), Constants.TICK * 1000);
            nextSocket.setSoTimeout(Constants.TICK * 1000);
            try (DataOutputStream os = new DataOutputStream(nextSocket.getOutputStream())) {
                Thread.sleep(new Random().nextInt(Constants.TICK * 10));
                token.macBytes = this.macBytes;
                token.nDigits = token.digits.length;
                PiHolder.getInstance().addNDigits(token.digits);
                os.write(token.toBytes());
                System.out.println("<- <> send token "
                                   + "version = " + token.version
                                   + ", MAC = " + token.macAddr
                                   + ", nDigits = " + token.nDigits);
            } catch (InterruptedException | IOException e) {
                System.out.println("<- ?? couldn't send token: " + e.getMessage());
            }
        } catch (IOException e) {
            if (state != State.CONFIG) {
                System.out.println("<- !! send failed " + e.getMessage());
                initReconfigure(version + 1);
            }
        }
    }

    private void initReconfigure(int newVersion) {
        if (state != State.CONFIG) {
            System.out.println("R!! init reconfigure v" + newVersion);
            while (this.version < newVersion) {
                sendReconfigure(this.macBytes, newVersion);
                try {
                    Thread.sleep(Constants.TICK * 500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            sendReconfigure(this.macBytes, newVersion);
        }
    }
}
