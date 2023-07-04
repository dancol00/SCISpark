package it.unisa.di.dif.pattern;

import it.unisa.di.dif.utils.CHILogger;
import it.unisa.di.dif.utils.Constant;
import org.jetbrains.bio.npy.NpyArray;
import org.jetbrains.bio.npy.NpzFile;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This class is an abstract class that implements the Pattern interface implementing the common method for all the kind of pattern
 *
 * @author Andrea Bruno
* @author Paola Capasso
 */
public abstract class GenericPattern implements Pattern {
    private ColorChannel red;
    private ColorChannel green;
    private ColorChannel blue;

    private final CHILogger logger = CHILogger.getInstance();
    private final Constant constant = Constant.getInstance();

    /**
     * Generate a new Noise pattern with the specified height and width
     *
     * @param height the height of the new noise pattern
     * @param width  the width of the new noise pattern
     */
    public GenericPattern(int height, int width) {
        float[][] red_data = new float[height][width];
        float[][] green_data = new float[height][width];
        float[][] blue_data = new float[height][width];
        this.red = new ColorChannel(red_data, ColorChannel.Color.RED);
        this.green = new ColorChannel(green_data, ColorChannel.Color.GREEN);
        this.blue = new ColorChannel(blue_data, ColorChannel.Color.BLUE);
        this.clear();
    }

    /**
     * Default constructor
     */
    public GenericPattern() {}

    @Override
    public ColorChannel getColorChannel(ColorChannel.Color color) {
        switch (color) {
            case RED:
                return red;
            case GREEN:
                return green;
            case BLUE:
                return blue;
            default:
                throw new IllegalArgumentException("Invalid channel");
        }
    }

    @Override
    public ColorChannel getRedChannel() {
        return getColorChannel(ColorChannel.RED);
    }

    @Override
    public ColorChannel getGreenChannel() {
        return getColorChannel(ColorChannel.GREEN);
    }

    @Override
    public ColorChannel getBlueChannel() {
        return getColorChannel(ColorChannel.BLUE);
    }

    @Override
    public int getHeight() {
        return green.getHeight();
    }

    @Override
    public int getWidth() {
        return green.getWidth();
    }

    @Override
    public float getValue(int x, int y, ColorChannel.Color color) {
        switch (color) {
            case RED:
                return red.getValue(x, y);
            case GREEN:
                return green.getValue(x, y);
            case BLUE:
                return blue.getValue(x, y);
            default:
                throw new IllegalArgumentException("Invalid channel");
        }
    }

    @Override
    public int getValueAsInteger(int x, int y, ColorChannel.Color color) {
        return (int) getValue(x, y, color);
    }

    @Override
    public void clear() {
        if (red != null) {
            red.clear();
        }
        if (green != null) {
            green.clear();
        }
        if (blue != null) {
            blue.clear();
        }
    }

    @Override
    public String getChannelName(ColorChannel.Color color) {
        switch (color) {
            case RED:
                return "Red";
            case GREEN:
                return "Green";
            case BLUE:
                return "Blue";
            default:
                throw new IllegalArgumentException("Invalid channel");
        }
    }

    @Override
    public void setHeight(int height) {}

    @Override
    public void setWidth(int width) {}

    @Override
    public void setChannel(ColorChannel value) {
        switch (value.getChannel()) {
            case RED:
                red = value;
                break;
            case GREEN:
                green = value;
                break;
            case BLUE:
                blue = value;
                break;
            default:
                throw new IllegalArgumentException("Invalid channel");
        }
    }

    @Override
    public void setRedChannel(ColorChannel value)
    {
        if (value.getChannel() != ColorChannel.Color.RED ){
            throw new IllegalArgumentException("Invalid channel");
        }
        setChannel(value);
    }

    @Override
    public void setGreenChannel(ColorChannel value) {
        if (value.getChannel() != ColorChannel.Color.GREEN ){
            throw new IllegalArgumentException("Invalid channel");
        }
        setChannel(value);
    }

    @Override
    public void setBlueChannel(ColorChannel value) {
        if(value.getChannel() != ColorChannel.Color.BLUE ){
            throw new IllegalArgumentException("Invalid channel");
        }
        setChannel(value);
    }

    @Override
    public void storeNoise(File f, boolean asInt) {
        if(f == null) {
            logger.log.warn("File is null");
            throw new IllegalArgumentException("File is null");
        }

        try (PrintStream ps = new PrintStream(f)) {

            ps.print(Constant.LINE_START_FOR_INFO_IN_NOISE_FILE + Constant.VALUE_SEPARATOR_FOR_NOISE_FILE +
                    getWidth() + Constant.VALUE_SEPARATOR_FOR_NOISE_FILE + getHeight() + "\n");

            if(asInt) {
                printChannelAsInt(ColorChannel.RED, ps);
                printChannelAsInt(ColorChannel.GREEN, ps);
                printChannelAsInt(ColorChannel.BLUE, ps);
            } else {
                printChannel(ColorChannel.RED, ps);
                printChannel(ColorChannel.GREEN, ps);
                printChannel(ColorChannel.BLUE, ps);
            }
        } catch (FileNotFoundException e) {
            if (constant.isWriteMessageLogOnConsole()) {
                e.printStackTrace();
            }
            logger.log.fatal("File not found: " + f.getAbsolutePath());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeAsInteger(File f) {
        storeNoise(f, true);
    }

    private void printChannel(ColorChannel.Color color, PrintStream ps) {
        switch (color) {
            case RED:
                red.print(ps);
                break;
            case GREEN:
                green.print(ps);
                break;
            case BLUE:
                blue.print(ps);
                break;
            default:
                throw new IllegalArgumentException("Invalid channel");
        }
    }

    private void printChannelAsInt(ColorChannel.Color color, PrintStream ps) {
        switch (color) {
            case RED:
                red.printAsInt(ps);
                break;
            case GREEN:
                green.printAsInt(ps);
                break;
            case BLUE:
                blue.printAsInt(ps);
                break;
            default:
                throw new IllegalArgumentException("Invalid channel");
        }
    }

    @Override
    public void storeAsFloat(File f) {
        storeNoise(f, false);
    }

    @Override
    public boolean equalsSize(Pattern other) {
        return other != null && this.getHeight() == other.getHeight() && this.getWidth() == other.getWidth();
    }

    @Override
    public boolean equalsChannel(Pattern other, ColorChannel.Color color) {
        return this.getColorChannel(color).equals(other.getColorChannel(color));
    }

    @Override
    public void crop(int width, int height) {
        if(width < 0 || height < 0) {
            throw new IllegalArgumentException("Width and height must be greater than 0");
        }
        red = new ColorChannel(red.getCentralCropping(width, height), ColorChannel.RED);
        green = new ColorChannel(green.getCentralCropping(width, height), ColorChannel.GREEN);
        blue = new ColorChannel(blue.getCentralCropping(width, height), ColorChannel.BLUE);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Pattern oth = (Pattern) obj;

        return equalsSize(oth) && equalsChannel(oth, ColorChannel.Color.RED) && equalsChannel(oth, ColorChannel.Color.GREEN) && equalsChannel(oth, ColorChannel.Color.BLUE);
    }

    @Override
    public String toString() {
        return "GenericPattern [width=" + this.getWidth() + ", height=" + this.getHeight() + "]";
    }

    /**
     * It adds the values of the pattern to the values of the current pattern
     *
     * @param pattern The pattern to add to this pattern.
     */
    public void add(GenericPattern pattern) {
        if(!equalsSize(pattern)) {
            throw new IllegalArgumentException("Patterns must be of the same size");
        }

        for(ColorChannel.Color color : ColorChannel.Color.values()) {
            for(int i = 0; i < this.getHeight(); i++) {
                for(int j = 0; j < this.getWidth(); j++) {
                    this.getColorChannel(color).setValue(i, j,
                            this.getColorChannel(color).getValue(i, j) + pattern.getColorChannel(color).getValue(i, j));
                }
            }
        }
    }

    /**
     * For each channel, for each row, for each column, subtract the value of the pixel in the pattern from the value of
     * the pixel in the current pattern.
     *
     * @param pattern The pattern to subtract from this pattern.
     */
    public void subtract(GenericPattern pattern) {
        if(!equalsSize(pattern)) {
            throw new IllegalArgumentException("Patterns must be of the same size");
        }

        for(ColorChannel.Color color : ColorChannel.Color.values()) {
            for(int i = 0; i < this.getHeight(); i++) {
                for(int j = 0; j < this.getWidth(); j++) {
                    this.getColorChannel(color).setValue(i, j,
                            this.getColorChannel(color).getValue(i, j) - pattern.getColorChannel(color).getValue(i, j));
                }
            }
        }
    }

    /**
     * It multiplies the values of the current pattern with the values of the pattern passed as an argument
     *
     * @param pattern The pattern to multiply with
     */
    public void multiply(GenericPattern pattern) {
        if(!equalsSize(pattern)) {
            throw new IllegalArgumentException("Patterns must be of the same size");
        }

        for(ColorChannel.Color color : ColorChannel.Color.values()) {
            for(int i = 0; i < this.getHeight(); i++) {
                for(int j = 0; j < this.getWidth(); j++) {
                    this.getColorChannel(color).setValue(i, j,
                            this.getColorChannel(color).getValue(i, j) * pattern.getColorChannel(color).getValue(i, j));
                }
            }
        }
    }

    /**
     * Divide the values of the current pattern by the values of the given pattern.
     *
     * @param pattern The pattern to divide this pattern by.
     */
    public void divide(GenericPattern pattern) {
        if(!equalsSize(pattern)) {
            throw new IllegalArgumentException("Patterns must be of the same size");
        }

        for(ColorChannel.Color color : ColorChannel.Color.values()) {
            for(int i = 0; i < this.getHeight(); i++) {
                for(int j = 0; j < this.getWidth(); j++) {
                    float value = pattern.getColorChannel(color).getValue(i, j) == 0 ? 0 :
                            this.getColorChannel(color).getValue(i, j) / pattern.getColorChannel(color).getValue(i, j);
                    this.getColorChannel(color).setValue(i, j, value);
                }
            }
        }
    }

    /**
     * Divide each pixel in the pattern by a value.
     *
     * @param value The value to divide the image by.
     */
    public void divideByValue(float value) {
        if(value == 0) {
            throw new IllegalArgumentException("Value must be non-zero");
        }

        for(ColorChannel.Color color : ColorChannel.Color.values()) {
            for(int i = 0; i < this.getHeight(); i++) {
                for(int j = 0; j < this.getWidth(); j++) {
                    float r = this.getColorChannel(color).getValue(i, j) / value;
                    this.getColorChannel(color).setValue(i, j, r);
                }
            }
        }
    }

    @Override
    public abstract GenericPattern getCroppedPattern(int width, int height);

    @Override
    public void storeAsNpz(Path p, String name) {
            float [] data = new float[this.getHeight() * this.getWidth() * 3];
            int idx = 0;
            for(ColorChannel.Color c : ColorChannel.Color.values()) {
                float[][] channel = this.getColorChannel(c).getData();
                int height = channel.length;
                int width = channel[0].length;
                for(int i = 0; i < height; i++) {
                    for(int j = 0; j < width; j++) {
                        int dst = (idx * width * height) + (i * width) + j;
                        data[dst] = channel[i][j];

                    }
                }
                idx++;
            }
            NpzFile.Writer w = NpzFile.write(p,true);
            int [] shape = {this.getHeight(), this.getWidth(), idx};
            w.write(name, data, shape);
            w.close();
    }

    @Override
    public void storeAsNpz(String filename, String name) {
        storeAsNpz(Paths.get(filename), name);
    }

    @Override
    public void storeAsNpz(Path p) {
        storeAsNpz(p, "data");
    }

    @Override
    public void storeAsNpz(String filename) {
        this.storeAsNpz(filename, "data");
    }

    @Override
    public float[][][] loadFromNpz(Path p) {
        return loadFromNpz(p, "data");
    }

    @Override
    public float[][][] loadFromNpz(String filename) {
        return loadFromNpz(Paths.get(filename), "data");
    }

    @Override
    public float[][][] loadFromNpz(Path p, String name) {
        NpzFile.Reader r = NpzFile.read(p);
        NpyArray data = r.get(name,1<<18);
        r.close();
        int height = data.getShape()[0];
        int width = data.getShape()[1];
        int channels = data.getShape()[2];
        float[][][] result = new float[height][width][channels];
        float [] result_array = data.asFloatArray();
        for(int k=0; k<channels; k++){
            for(int i=0; i<height; i++){
                for(int j=0; j<width; j++){
                    int dst = (k * width * height) + (i * width) + j;
                    result[i][j][k] = result_array[dst];
                }
            }
        }
        return result;
    }

    public static ColorChannel[] getColorChannelsFromFloatarray(float[][][] data) {
        int height = data.length;
        int width = data[0].length;
        int chan_no = data[0][0].length;
        ColorChannel[] channels = new ColorChannel[chan_no];
        for(int i = 0; i < chan_no; i++) {
            float [][] channel = new float[height][width];
            for(int j = 0; j < height; j++) {
                for (int k = 0; k < width; k++) {
                    channel[j][k] = data[j][k][i];
                }
            }
            channels[i] = new ColorChannel(channel, ColorChannel.getColorFromIndex(i));
        }
        return channels;
    }

    @Override
    public float[][][] loadFromNpz(String filename, String name) {
        return loadFromNpz(Paths.get(filename), name);
    }

    public void initFromNpz(Path p, String name) {
        if(name==null)
            name="data";
        float [][][] data = this.loadFromNpz(p, name);
        ColorChannel[] channels = GenericPattern.getColorChannelsFromFloatarray(data);
        for (ColorChannel channel : channels) {
            this.setChannel(channel);
        }
    }
    public void initFromNpz(String filename, String name) {
        this.initFromNpz(Paths.get(filename), name);
    }
}
