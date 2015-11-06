package ifmo.ctddev.efimova.net.token;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Test {

    private static volatile boolean isShouldStopExecution = false;

    public static void main(String[] args) {
        /*
        final int[] i = {0};
        Runnable thread2 = () -> {
            while (true) {
                System.out.println("Pfffffffffff i = " + i[0]);
                i[0]++;
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(thread2);
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */

        /*
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(thread2).get(Constants.TICK, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            System.out.println("Reconfigure timeout");
        }
        */

        /*
        Thread thread3 = new Thread(() -> {
            while (!Thread.interrupted()) {
                System.out.println("Pfffffffffff i = " + i[0]);
                i[0]++;
            }
        });

        Thread thread4 = new Thread(() -> {
            try {
                Thread.sleep(1);
                thread3.interrupt();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread3.start();
        thread4.start();
        */

        /*
        final int[] i = {0};

        Runnable thread2 = () -> {
            while (!isShouldStopExecution) {
                System.out.println("Pfffffffffff i = " + i[0]);
                i[0]++;
            }

            System.out.println("FINISH THREAD");
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(thread2).get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            isShouldStopExecution = true;
        }

        executor.shutdownNow();
        */

        Socket nextSocket = new Socket();
        try {
            nextSocket.connect(new InetSocketAddress("192.168.0.3", Constants.TCP_PORT), Constants.TICK * 1000);
            DataOutputStream os = new DataOutputStream(nextSocket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
