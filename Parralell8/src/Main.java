public class Main {

    public static void main(String[] args) {
        MessageBox box = new MessageBox();

        Producer producer = new Producer(box, 5);
        Consumer consumer = new Consumer(box);

        producer.start();
        consumer.start();

        try {
            producer.join();
            consumer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[main] both threads finished");
    }
}

class MessageBox {
    private String message;

    private boolean hasMessage = false;

    public synchronized void put(String msg) {
        String name = Thread.currentThread().getName();
        try {
            while (hasMessage) {
                System.out.println("[" + name + "] waiting: buffer is full");
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        message = msg;
        hasMessage = true;
        System.out.println("[" + name + "] put: " + msg);
        notifyAll();
    }


    public synchronized String take() {
        String name = Thread.currentThread().getName();
        try {
            while (!hasMessage) {
                System.out.println("[" + name + "] waiting: buffer is empty");
                wait();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }

        String result = message;
        message = null;
        hasMessage = false;
        System.out.println("[" + name + "] took: " + result);
        notifyAll();
        return result;
    }
}

class Producer extends Thread {

    private final MessageBox box;
    private final int count;

    public Producer(MessageBox box, int count) {
        super("Producer");
        this.box = box;
        this.count = count;
    }

    @Override
    public void run() {
        for (int i = 1; i <= count; i++) {
            box.put("Message #" + i);
        }

        box.put("Done");
        System.out.println("[" + getName() + "] finished");
    }
}

class Consumer extends Thread {

    private final MessageBox box;

    public Consumer(MessageBox box) {
        super("Consumer");
        this.box = box;
    }

    @Override
    public void run() {
        while (true) {
            String msg = box.take();

            if (msg == null) {
                break;
            }

            System.out.println("[" + getName() + "] received: " + msg);

            if (msg.equals("Done")) {
                break;
            }
        }
        System.out.println("[" + getName() + "] finished");
    }
}