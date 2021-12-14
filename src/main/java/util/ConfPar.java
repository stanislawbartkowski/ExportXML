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
    private final String where = "where";
    private final String query = "query";
    private final String commit = "commit";

    private final String asblob = "asblob";
    private final String asstring = "asstring";

    private final String destprefix = "dest_";
    private final String destinsert = destprefix + "insert";

    public String getURL() {
        return prop.getProperty(url);
    }


    public String getWhere() {
        return prop.getProperty(where);
    }

    public String getUser() {
        return prop.getProperty(user);
    }

    public String getPassword() {
        return prop.getProperty(password);
    }

    public String getDestURL() {
        return prop.getProperty(destprefix + url);
    }

    public String getDestUser() {
        return prop.getProperty(destprefix + user);
    }

    public String getDestPassword() {
        return prop.getProperty(destprefix + password);
    }

    public String getDestInsert() {
        return prop.getProperty(destinsert);
    }

    public int getCommit() {
        return Integer.parseInt(prop.getProperty(commit,"10000"));
    }

    public String getDir() {
        return prop.getProperty(outdir);
    }

    public String getDel() {
        return prop.getProperty(del, "~");
    }

    public String getIdCol() {
        return prop.getProperty(idcol);
    }

    public String getQuery() {
        return prop.getProperty(query);
    }

    public String getXmlCol() {
        String[] id = prop.getProperty(idxml).split(",");
        return id[0];
    }

    public boolean readXmlasblob() {
        String[] id = prop.getProperty(idxml).split(",");
        if (id.length == 1) return true;
        return id[1].equals(asblob);
    }

    public int getCounter() {
        String s = prop.getProperty(counter, "1000");
        int i = Integer.parseInt(s);
        return i;
    }

    private void checkpar(String p, String filename) {
        if (prop.getProperty(p) == null) {
            Log.severe(filename + " " + p + " parameter expected");
        }
    }

    public void load(String filename, boolean checkdest) {
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
        if (checkdest) {
            checkpar(destprefix + url, filename);
            checkpar(destprefix + user, filename);
            checkpar(destprefix + password, filename);
            checkpar(destinsert, filename);
        }
        if (getQuery() != null) Log.info(String.format("Use query: %s", getQuery()));
        // verify blob
        String[] id = prop.getProperty(idxml).split(",");
        if (id.length > 1) {
            String m = id[1];
            if (!m.equals(asstring) && !m.equals(asblob)) {
                String errmess = prop.getProperty(idxml) + " method specifiers expected as %s or %s ".format(asblob, asstring);
                Log.severe(errmess);
            }
        }
        Log.info(String.format("Read XML columns as %s", readXmlasblob() ? " as blob" : "as string"));
        Log.info("User: " + getUser());
        Log.info("Password:XXXXX");
        Log.info("Url: " + getURL());
        Log.info("Directory for text files:" + getDir());
        Log.info("Delimiter:" + getDel());
        Log.info("Id column:" + getIdCol());
        Log.info(("XML column: " + getXmlCol()));
        if (checkdest) {
            Log.info("Moving data to target table");
            Log.info("Dest user: " + getDestUser());
            Log.info("Dest password:XXXXX");
            Log.info("Dest url: " + getDestURL());
            Log.info(String.format("Commit after %d ",getCommit()));
        }
    }
}
