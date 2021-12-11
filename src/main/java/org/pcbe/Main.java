package org.pcbe;

import com.google.gson.Gson;
import dnl.utils.text.table.TextTable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.swing.Timer;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.time.Duration;
import java.util.*;

public class Main {

    public static void main(String[] args) {

        InitializeFiles.checkDir();
        InitializeFiles.checkFile();

        Map<String,TopicMessageStock> stocks = new HashMap<String,TopicMessageStock>();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("plutus"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                    TopicMessageStock topicMessageStock = new Gson().fromJson(record.value(),TopicMessageStock.class);
                    stocks.put(record.key(),topicMessageStock);
                Timer timer = new javax.swing.Timer(5000, new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        if (stocks.size() > 0) {
                            InitializeTable initializeTable = new InitializeTable(stocks);
                        }
                    }
                });
                timer.setRepeats(true);
                timer.start();
            }
        }
    }
    private static class TopicMessageStock {
        private int quantity;
        private float price;

        public TopicMessageStock(int quantity, float price) {
            this.quantity = quantity;
            this.price = price;
        }
    }
    public static class InitializeFiles {
        private static String path = System.getProperty("user.home") + File.separator +
                                     "Documents"+File.separator + "Plutus-UI";
        private static File customDir = new File(path);
        private static File customFile = new File(customDir,"table.txt");
        public static void checkDir() {
            if (customDir.exists()) {
                System.out.println(customDir + " already exists");
            } else if (customDir.mkdirs()) {
                System.out.println(customDir + " was created");
            } else {
                System.out.println(customDir + " was not created");
            }
        }
        public static void checkFile() {
            if (customFile.exists()) {
                System.out.println(customFile + " already exists");
            } else {
                try {
                    if (customFile.createNewFile()) {
                        System.out.println(customFile + " was created");
                    } else {
                        System.out.println(customFile + " was not created");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static class InitializeTable {

        private TextTable tt;

        public InitializeTable(Map<String,TopicMessageStock> stocks) {
            final String[] labels= {"Stock", "Price" , "Quantity" };
            Object[][] data = new Object[stocks.size()][3];
            List<String> keys = new ArrayList<String>(stocks.keySet());
            for(int i=0;i<keys.size();i++){
                data[i][0]=keys.get(i);
                data[i][1] = stocks.get(keys.get(i)).price;
                data[i][2] = stocks.get(keys.get(i)).quantity;
            }
            TextTable tt = new TextTable(labels, data);
            tt.setAddRowNumbering(true);
            tt.setSort(0);
//            tt.printTable();
            try {
                OutputTable.writeTable(InitializeFiles.path, tt);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
    public static class OutputTable {
        public static void writeTable(String path, TextTable tt) throws IOException {
            FileOutputStream outfile=new FileOutputStream(path + File.separator + "table.txt");
            PrintStream p = new PrintStream( outfile);
            tt.printTable(p, 0);
        }
    }

}
