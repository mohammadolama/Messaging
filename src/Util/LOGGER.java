package Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

public class LOGGER {
    public static void BrokerLog(String string) {
        File file = new File("Log/");
        if (!file.isDirectory()) {
            file.mkdirs();
        }
        String st = String.format("Log/broker.log");
        Calendar calendar = Calendar.getInstance();
        try {
            FileWriter fileWriter = new FileWriter(st, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            Locale locale = new Locale("English", "England");
            SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss", locale);
            bufferedWriter.write(formatter.format(calendar.getTime()) + "  " + string + "\n");
            bufferedWriter.close();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}