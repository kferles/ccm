package config;

/**
 * Created by kostas on 4/15/14.
 */
public class ConfigParameters {
    private static ConfigParameters ourInstance = new ConfigParameters();

    private int bufferSize = 1024;

    private int maxBuffNumber = 100;

    private boolean durable = false;

    private ConfigParameters() {
    }

    public static ConfigParameters getInstance() {
        return ourInstance;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getMaxBuffNumber() {
        return maxBuffNumber;
    }

    public void setMaxBuffNumber(int maxBuffNumber) {
        this.maxBuffNumber = maxBuffNumber;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

}
