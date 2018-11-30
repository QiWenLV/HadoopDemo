package com.zqw.lock;

public class Test1 {
    static int n = 500;

    //    private static String connectString = "hadoop21:2181,hadoop22:2181,hadoop23:2181";
    private static String config = "hadoop24:2181";

    public static void secskill() {
        System.out.println(--n);
    }

    public static void main(String[] args) {

//
//        Runnable runnable = new Runnable() {
//            public void run() {
//                DistributedLock lock = null;
//                try {
//                    lock = new DistributedLock(config, "test1");
//                    lock.lock();
//                    secskill();
//                    System.out.println(Thread.currentThread().getName() + "正在运行");
//                } finally {
//                    if (lock != null) {
//                        lock.unlock();
//                    }
//                }
//            }
//        };
        Runnable runnable = new Runnable() {
            public void run() {
                TestService lock = null;
                try {
                    lock = new TestService(config, "aaa");
                    lock.createNode();

                    System.out.println(Thread.currentThread().getName() + "正在运行");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
//                    if (lock != null) {
//                        lock.unlock();
//                    }
                }
            }
        };

        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }
    }
}
