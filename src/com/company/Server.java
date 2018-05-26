package com.company;

import java.io.*;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.*;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.opencsv.CSVWriter;
import com.google.gson.Gson;


public class Server {
    private HashSet<ConcurrentLinkedQueue<String>> QueArr = new HashSet<>();
    private int SWcount = 0;
    private int OBJcount = 0;
    private static long currTimestamp;
    public static Map<Socket, Long> timeDiffs = new ConcurrentHashMap<>();
    private static Map<Socket, PrintWriter> writers = new ConcurrentHashMap<>();
    private static Map<Socket, BufferedReader> readers = new ConcurrentHashMap<>();
    public static Map<Socket, Boolean> status = new ConcurrentHashMap<>();
    private static Map<Socket, Boolean> Accept = new ConcurrentHashMap<>();
    public static Map<Socket, Boolean> isCali = new ConcurrentHashMap<>();
    private static Map<Socket, Long> lasttimestamps = new ConcurrentHashMap<>();
    public static Map<InetAddress, count> allCounts = new ConcurrentHashMap<>();
    public static Map<InetAddress, Map<String, count>> allCountMaps = new ConcurrentHashMap<>();
    private ServerSocket server;

    private Server(String ipAddress) throws Exception {
        if (ipAddress != null && !ipAddress.isEmpty())
            this.server = new ServerSocket(8888, 10, InetAddress.getByName(ipAddress));
        else
            this.server = new ServerSocket(8888, 10, InetAddress.getLocalHost());
    }

    private void create(Socket client, String clientAddress, ConcurrentLinkedQueue<String> sq, int num, ExecutorService pool, ConcurrentLinkedQueue<wrapper> que) {
        Runnable sw = new sw_task(client, clientAddress, sq, num, que);
        pool.execute(sw);
    }

    private void create(Socket client, String clientAddress, HashSet<ConcurrentLinkedQueue<String>> Arr, ExecutorService pool, int idx, ConcurrentLinkedQueue<wrapper> que) {
        count cc;
        if (allCounts.containsKey(client.getInetAddress())) {
            cc = allCounts.get(client.getInetAddress());
        } else {
            cc = new count();
            allCounts.put(client.getInetAddress(), cc);
        }
        Sensordata sd = new Sensordata();
        Runnable node = new node_task(client, clientAddress, cc, Arr, sd, idx, que);
        pool.execute(node);
    }

    private void listen(ExecutorService pool, ConcurrentLinkedQueue<wrapper> que) throws Exception {
        System.out.println("listening...");
        while (true) {
            Socket client = this.server.accept();
            String clientAddress = client.getInetAddress().getHostAddress();
            System.out.println("connected with" + clientAddress);
            BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(client.getOutputStream()));
            long timeDiff = 0;
            timeDiffs.put(client, timeDiff);
            writers.put(client, writer);
            readers.put(client, reader);
            status.put(client, false);
            isCali.put(client, false);
            lasttimestamps.put(client, System.currentTimeMillis());
            Thread.sleep(10);
            writer.println("TYPE");
            writer.flush();
            System.out.println("sent type");
            while (true) {
                String data = reader.readLine();
                if (data == null || data.isEmpty()) {
                    continue;
                } else if (data.matches(".*OBJ")) {
                    this.OBJcount++;
                    Accept.put(client, true);
                    Thread.sleep(2000);
                    create(client, clientAddress, this.QueArr, pool, this.OBJcount, que);
                    break;
                } else if (data.matches(".*SWT")) {
                    this.SWcount++;
                    ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
                    this.QueArr.add(queue);
                    Accept.put(client, true);
                    Thread.sleep(2000);
                    create(client, clientAddress, queue, this.SWcount, pool, que);
                    break;
                }

            }
        }

    }

    private InetAddress getSocketAddress() {
        return this.server.getInetAddress();
    }

    private int getPort() {
        return this.server.getLocalPort();
    }

    public static void main(String[] args) throws Exception {
        ConcurrentLinkedQueue<wrapper> que = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(12);
        TimeLimiter timeLimiter = SimpleTimeLimiter.create(pool);
        Server app = new Server("10.42.0.10");
        System.out.println("\r\nRunning Server: " +
                "Host=" + app.getSocketAddress().getHostAddress() +
                " Port=" + app.getPort());
        Runnable timer = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(500);
                        if (!writers.isEmpty() && !writers.isEmpty() && !readers.isEmpty() && !Accept.isEmpty()) {
                            for (Map.Entry entry : writers.entrySet()) {
                                if (!((Socket) entry.getKey()).isClosed()) {
                                    PrintWriter writer = (PrintWriter) entry.getValue();
                                    BufferedReader reader = readers.get(entry.getKey());
                                    long timeDiff = timeDiffs.get(entry.getKey());
                                    currTimestamp = System.currentTimeMillis();
                                    long lastTimestamp = lasttimestamps.get(entry.getKey());
                                    if (Accept.containsKey(entry.getKey()) && status.containsKey(entry.getKey()) && (timeDiff == 0 || currTimestamp - lastTimestamp > 0.1 * 60000) && !status.get(entry.getKey()) && Accept.get(entry.getKey())) {
                                        isCali.put((Socket) entry.getKey(), true);
                                        timeDiff = 0;
                                        lasttimestamps.put((Socket) entry.getKey(), currTimestamp);
                                        writer.println("time");
                                        writer.flush();
                                        System.out.println("calibration request sent");
                                        while (true) {
                                            try {
                                                String timeMSG = timeLimiter.callWithTimeout(reader::readLine, 3, TimeUnit.SECONDS);
                                                System.out.println(timeMSG);
                                                if (timeMSG == null || timeMSG.isEmpty()) {
                                                    ((Socket)entry.getKey()).close();
                                                    System.out.println("closing socket...");
                                                    break;
                                                }
                                                if (timeMSG.matches(".*T")) {
                                                    timeDiff += (System.currentTimeMillis() - Long.parseLong(timeMSG.substring(0, timeMSG.length() - 1))) % 1000000;
                                                }
                                                if (timeMSG.equals("Q")) {
                                                    timeDiff /= 10;
                                                    timeDiffs.put((Socket) entry.getKey(), timeDiff);
                                                    isCali.put((Socket) entry.getKey(), false);
                                                    break;
                                                }
                                                if (timeMSG.matches(".*t")) {
                                                    timeDiff += (System.currentTimeMillis() - Long.parseLong(timeMSG.substring(0, timeMSG.length() - 1))) % 1000000;
                                                }
                                                if (timeMSG.equals("q")) {
                                                    timeDiff /= 10;
                                                    timeDiffs.put((Socket) entry.getKey(), timeDiff);
                                                    isCali.put((Socket) entry.getKey(), false);
                                                    break;
                                                }
                                            } catch (TimeoutException | UncheckedIOException e) {
                                                System.out.println("lost connection, closing...");
                                                ((Socket) entry.getKey()).close();
                                                System.out.println("closing this socket...");
                                                break;
                                            } catch (ExecutionException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                        System.out.println("calibration finished with difference_" + timeDiff);
                                    }
                                }
                            }
                        }

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        Runnable writer = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    if (que != null && que.peek() != null) {
                        wrapper wrap = que.poll();
                        String path = "";
                        if (wrap != null) {
                            if (wrap.label != null) {
                                path = "/home/yan/Sensordata/" + wrap.label.substring(wrap.label.length() - 2) + "/";
                            } else {
                                path = "/home/yan/Sensordata/" + wrap.ip.substring(wrap.ip.length() - 2) + "/";
                            }
                            String DirectoryName = path.concat("experiment_" + String.valueOf(wrap.cc));
                            String fileName = wrap.type + "_" + wrap.idx + "_" + wrap.timeDiff + "_" + wrap.ip + ".csv";
                            File directroy = new File(DirectoryName);
                            if (!directroy.exists()) {
                                System.out.println(wrap.type + " creating " + DirectoryName);
                                directroy.mkdirs();
                                System.out.println(DirectoryName);
                            }
                            String Path = DirectoryName + "/" + fileName;
                            try {
                                System.out.println(wrap.type + " writing " + DirectoryName);
                                synchronized (wrapper.class) {
                                    wrap.da.writeToCSV(Path);
                                }
                                System.out.println("Thread_" + wrap.type + "_" + wrap.ip + "_" + "fininshed writing");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        };
        pool.execute(writer);
        pool.execute(timer);
        app.listen(pool, que);
    }
}

class count {
    private int count;

    public synchronized void increment() {
        this.count++;
    }

    public synchronized int get() {
        return this.count;
    }
}

class node_task implements Runnable {
    private Socket client;
    private String ip;
    private count cc;
    private Sensordata sd;
    private HashSet<ConcurrentLinkedQueue<String>> arr;
    private int idx;
    private ConcurrentLinkedQueue<wrapper> que;

    public node_task(Socket client, String ip, count cc, HashSet<ConcurrentLinkedQueue<String>> arr, Sensordata sd, int idx, ConcurrentLinkedQueue<wrapper> que) {
        this.client = client;
        this.ip = ip;
        this.cc = cc;
        this.arr = arr;
        this.sd = sd;
        this.idx = idx;
        this.que = que;
    }

    public void run() {
        try {
            System.out.println("Object thread start");
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.client.getInputStream()));
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(this.client.getOutputStream()));
            while (true) {
                if (client.isClosed()) {
                    System.out.println("Socket closed terminating...");
                    break;
                }
                if (!Server.isCali.get(client)) {
                    String message = reader.readLine();
                    if (message == null || message.isEmpty()) {
                        continue;
                    } else if (message.equals("S")) {
                        Server.status.put(client, true);
                        System.out.println("received start " + String.valueOf(System.currentTimeMillis()));
                        this.cc.increment();
                        String sign_start = "start_" + this.ip.substring(this.ip.length() - 3);
                        for (ConcurrentLinkedQueue<String> queue : arr) {
                            while (true) {
                                if (queue.offer(sign_start)) {
                                    System.out.println("put a " + sign_start);
                                    break;
                                }
                            }
                        }
                        while (true) {
                            if (!Server.isCali.get(client)) {
                                String ss = reader.readLine();
                                if (ss == null || ss.isEmpty()) {
                                    continue;
                                } else {
                                    if (ss.equals("E")) {
                                        System.out.println("stop in stack");
                                        String sign_stop = "stop_" + this.ip.substring(this.ip.length() - 3);
                                        for (ConcurrentLinkedQueue<String> queue : arr) {
                                            while (true) {
                                                if (queue.offer(sign_stop)) {
                                                    System.out.println("put a " + sign_stop);
                                                    break;
                                                }
                                            }
                                        }
                                        int c = this.cc.get();
                                        wrapper toPut = new wrapper();
                                        toPut.da = this.sd;
                                        toPut.cc = c;
                                        toPut.idx = this.idx;
                                        toPut.ip = this.ip;
                                        toPut.timeDiff = Server.timeDiffs.get(this.client);
                                        toPut.type = "OBJ";
                                        toPut.label = null;
                                        while (true) {
                                            if (this.que.offer(toPut)) {
                                                break;
                                            }
                                        }
                                        Server.status.put(client, false);
                                        break;
                                    }
                                    this.sd.write(ss);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class sw_task implements Runnable {
    private volatile int startCount = 0;
    private volatile boolean isStart = false;
    private Map<String, Sensordata> SenMap = new ConcurrentHashMap<>();
    private Map<String, count> CountMap;
    private Socket client;
    private String ip;
    private ConcurrentLinkedQueue<String> sq;
    private int swc;
    private ExecutorService pool;
    private ConcurrentLinkedQueue<wrapper> que;

    public sw_task(Socket client, String ip, ConcurrentLinkedQueue<String> sq, int num, ConcurrentLinkedQueue<wrapper> que) {
        this.client = client;
        this.ip = ip;
        this.sq = sq;
        this.swc = num;
        this.pool = Executors.newFixedThreadPool(10);
        this.que = que;
        if (Server.allCountMaps.containsKey(client.getInetAddress())) {
            this.CountMap = Server.allCountMaps.get(client.getInetAddress());
        } else {
            this.CountMap = new ConcurrentHashMap<>();
            Server.allCountMaps.put(client.getInetAddress(), this.CountMap);
        }
    }

    public void run() {
        try {
            System.out.println("watch thread start");
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.client.getInputStream()));
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(this.client.getOutputStream()));
            while (true) {
                if (client.isClosed()) {
                    System.out.println("socket closed terminating...");
                    break;
                }
                try {
                    if (this != null && sq != null && sq.peek() != null && sq.peek().startsWith("start")) {
                        synchronized (this) {
                            startCount++;
                            System.out.println("current startcount:" + startCount);
                        }
                        String sig = this.sq.poll();
                        String sigh = sig.substring(sig.length() - 3);
                        Sensordata mSen = new Sensordata();
                        if (this.SenMap.get(sigh) == null) {
                            this.SenMap.put(sigh, mSen);
                        }
                        if (this.CountMap.get(sigh) == null) {
                            count c = new count();
                            c.increment();
                            this.CountMap.put(sigh, c);
                        } else {
                            this.CountMap.get(sigh).increment();
                        }
                        System.out.println("thread_" + "Sw" + this.swc + " received " + sig);
                        System.out.println("current isStart:" + isStart);
                        if (!this.isStart) {
                            this.isStart = true;
                            Server.status.put(client, true);
                            Runnable bvn = new Runnable() {
                                @Override
                                public void run() {
                                    while (true) {
                                        if (client.isClosed()) {
                                            System.out.println("socket closed terminating...");
                                            break;
                                        }
                                        try {
                                            if (!Server.isCali.get(client)) {
                                                String data = reader.readLine();
                                                if (data == null || data.isEmpty()) {
                                                    continue;
                                                } else {
                                                    if (data.contains("stop")) {
                                                        isStart = false;
                                                        Server.status.put(client, false);
                                                        System.out.println("received stop from smart watch,terminating...");
                                                        break;
                                                    }
                                                    for (Map.Entry entry : SenMap.entrySet()) {
                                                        ((Sensordata) entry.getValue()).write(data);

                                                    }
                                                }
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            };
                            this.pool.execute(bvn);
                            writer.println("s");
                            writer.flush();
                            System.out.println("sending start signal to" + ip + " " + String.valueOf(System.currentTimeMillis()));
                        }
                        Runnable bvm = new Runnable() {
                            private String label = sigh;
                            private Sensordata Sen = SenMap.get(sigh);

                            @Override
                            public void run() {
                                System.out.println("thread_" + "Sw" + swc + "to wait stop created");
                                while (true) {
                                    if (client.isClosed()) {
                                        System.out.println("Socket closed terminating...");
                                        break;
                                    }
                                    try {
                                        if (this != null && sq != null && sq.peek() != null && (sq.peek().equals("stop_" + this.label))) {
                                            sq.poll();
                                            System.out.println("received_stop_" + this.label);
                                            System.out.println("sending stop signal to" + ip);
                                            startCount--;
                                            if (startCount == 0) {
                                                writer.println("e");
                                                writer.flush();
                                            }
                                            System.out.println("current start count:" + startCount);
                                            int c;
                                            c = CountMap.get(this.label).get();
                                            wrapper toPut = new wrapper();
                                            System.out.println(this.Sen.datas.isEmpty());
                                            toPut.da = this.Sen;
                                            toPut.cc = c;
                                            toPut.idx = swc;
                                            toPut.ip = ip;
                                            toPut.timeDiff = Server.timeDiffs.get(client);
                                            toPut.type = "SWT";
                                            toPut.label = this.label;
                                            while (true) {
                                                if (que.offer(toPut)) {
                                                    break;
                                                }
                                            }
                                            SenMap.remove(sigh);
                                            break;
                                        }
                                    } catch (NullPointerException e) {
                                    }
                                }
                            }
                        };
                        this.pool.execute(bvm);
                    }
                } catch (NullPointerException e) {
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class Sensordata {
    public ArrayList<String[]> datas = new ArrayList<>();
    private CSVWriter writer = null;

    public void write(String s) {
        synchronized (this) {
            Gson gson = new Gson();
            Type colloectionType = new TypeToken<ArrayList<String[]>>() {
            }.getType();
            try {
                ArrayList<String[]> ss = gson.fromJson(s, colloectionType);
                this.datas.addAll(ss);
            } catch (JsonSyntaxException e) {
                e.printStackTrace();
                System.out.println("json err -> " + s);
            }
        }
    }

    public void writeToCSV(String path) {
        synchronized (this) {
            try {
                this.writer = new CSVWriter(new FileWriter(path, true));
            } catch (Exception e) {
                e.printStackTrace();
            }
            this.writer.writeAll(this.datas);
            this.datas.clear();
            try {
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class wrapper {
    Sensordata da;
    String type;
    int idx;
    long timeDiff;
    String ip;
    int cc;
    String label;
}