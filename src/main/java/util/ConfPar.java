package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfPar {

    private Properties prop = new Properties();

    private final String url = "url";
    private final String user = "user";
    private final String password = "password";
    private final String outdir = "textdir";
    private final String del = "del";
    private final String idcol = "idcol";
    private final String idxml = "xmlcol";
    private final String counter = "counter";

    public String getURL() {
        return prop.getProperty(url);
    }

    public String getUser() {
        return prop.getProperty(user);
    }

    public String getPassword() {
        return prop.getProperty(password);
    }

    public String getDir() {
        return prop.getProperty(outdir);
    }

    public String getDel() {
        return prop.getProperty(del,"~");
    }

    public String getIdCol() {
        return prop.getProperty(idcol);
    }

    public String getXmlCol() {
        return prop.getProperty(idxml);
    }

    public int getCounter() {
        String s = prop.getProperty(counter,"1000");
        int i = Integer.parseInt(s);
        return i;
    }

    private void checkpar(String p, String filename) {
        if (prop.getProperty(p) == null) {
            Log.severe(filename + " " + p + " parameter expected");
        }
    }

    public void load(String filename) {
        try (InputStream input = new FileInputStream(filename)) {
            prop.load(input);
        } catch (IOException ex) {
            Log.severe("Failed while reading property file", ex);
        }
        checkpar(url, filename);
        checkpar(user, filename);
        checkpar(password, filename);
        checkpar(outdir, filename);
        checkpar(idcol, filename);
        checkpar(idxml, filename);
        Log.info("User: " + getUser());
        Log.info("Password:XXXXX");
        Log.info("Url: " + getURL());
        Log.info("Directory for text files:" + getDir());
        Log.info("Delimiter:" + getDel());
        Log.info("Id column:" + getIdCol());
        Log.info(("XML column: " + getXmlCol()));
    }
}
