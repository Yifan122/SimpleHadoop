package com.yifan.simple.hadoop;

import lombok.SneakyThrows;

import java.util.LinkedList;

public class FSEditLog {

    public boolean isSyncRunning = false; // 是否正在刷盘
    long maxTxid = 0;
    DoubleBuf doubleBuf = new DoubleBuf();
    ThreadLocal<Long> threadLocal = new ThreadLocal<>();
    boolean isWait = false; // 是否等待
    private long txid = 0;

    public static void main(String[] args) {
        FSEditLog fsEditLog = new FSEditLog();
        boolean isFlag = true;
        long txid = 0;
        while (isFlag) {
            txid++;
            if (txid == 100) {
                isFlag = false;
            }

            new Thread(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    boolean isFlag_2 = true;
                    long txid_2 = 0;
                    while (isFlag_2) {
                        txid_2++;
                        if (txid_2 == 100000) {
                            isFlag_2 = false;
                        }

                        fsEditLog.writeEditlog("metadata");
                    }
                }
            }).start();
        }
    }

    public void writeEditlog(String log) {
        // 分段锁
        synchronized (this) {
            txid++;
            threadLocal.set(txid);

            EditLog editLog = new EditLog(txid, log);
            doubleBuf.write(editLog);
        }
        // 耗时
        logFlush();
    }

    public void logFlush() {
        if (doubleBuf.bufCurrent.size() < 100) {
            return;
        }

        synchronized (this) {
            if (isSyncRunning) {
                if (threadLocal.get() < maxTxid) {
                    // 其他线程已经在写数据了
                    return;
                }

                // 如果threadLocal.get() >= maxTxid
                // 说明这是个新的任务， 但是当前任务正在运行，需要等待， 否则会出现线程不安全
                if (isWait) {
                    return;
                }

                // 最开始
                isWait = true;
                while (isSyncRunning) { // 线程1正在刷盘， 线程2过来了， 需要等待
                    try {
                        this.wait(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                isWait = false;
            }
            // 如果此时没有在刷盘，那么交换内存
            doubleBuf.swap();
            if (doubleBuf.bufReady.size() > 0) {
                maxTxid = doubleBuf.getMaxTxid();
            }
            isSyncRunning = true;
            System.out.println("Start flushing, txid: " + txid + " maxTxid: " + maxTxid);
        }
        doubleBuf.flush();
        synchronized (this) {
            isSyncRunning = false;
            // 唤醒等待的线程， 表明已经刷盘结束了
            this.notifyAll();
        }
    }

    public class EditLog {
        public long txid; // transction id
        public String log;

        public EditLog(long txid, String log) {
            this.txid = txid;
            this.log = log;
        }

        @Override
        public String toString() {
            return "EditLog{" +
                    "txid=" + txid +
                    ", log='" + log + '\'' +
                    '}';
        }
    }

    public class DoubleBuf {
        LinkedList<EditLog> bufCurrent = new LinkedList<>();
        LinkedList<EditLog> bufReady = new LinkedList<>();

        public void write(EditLog editLog) {
            bufCurrent.add(editLog);
        }

        public void flush() {
            for (EditLog editLog : bufReady) {
                System.out.println(editLog);
            }
            bufReady.clear();
        }

        public void swap() {
            // BufCurrent -> bufReady
            LinkedList<EditLog> tmp = bufReady;
            bufReady = bufCurrent;
            bufCurrent = tmp;
        }

        public long getMaxTxid() {
            return bufReady.getLast().txid;
        }
    }
}
