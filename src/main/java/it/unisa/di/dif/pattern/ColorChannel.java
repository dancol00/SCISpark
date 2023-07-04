package it.unisa.di.dif.pattern;

import it.unisa.di.dif.utils.Constant;

import java.io.PrintStream;
import java.util.Scanner;

/**
 * The ColorChannel class represents a single color channel of an image or pattern
 *
 * @see Pattern
 * @author Andrea Bruno
* @author Paola Capasso
 */
public class ColorChannel {
    public static final Color RED = Color.RED;
    public static final Color GREEN = Color.GREEN;
    public static final Color BLUE = Color.BLUE;

    /**
     * Defining a new type called Channel, which can only be one of three values: RED, GREEN, or BLUE.
     *
     */
    public enum Color {
        /**
         * Red color
         */
        RED,
        /**
         * Green color
         */
        GREEN,
        /**
         * Blue color
         */
        BLUE
    }

    private float[][] data;
    private Color color;

    /**
     * Generate a new instance of color channel using data values for pixel and for channel.
     *
     * @param data values for the pixels
     * @param color color of the channel
     */
    public ColorChannel(float[][] data, Color color) {
        this.data = data;
        this.color = color;
    }

    /**
     * Generate a new instance of color channel using data stored in channelStored.
     *
     * @param channelStored path on disk for stored channel.
     */
    public ColorChannel(String channelStored) {
        Scanner sc = new Scanner(channelStored).useDelimiter("\n");
        int i = 0;
        while (sc.hasNextLine())
        {
            String line = sc.nextLine();
            Scanner scLine = new Scanner(line).useDelimiter(Constant.VALUE_SEPARATOR_FOR_NOISE_FILE);
            if (line.startsWith(String.valueOf(Constant.LINE_START_FOR_CHANNEL_IN_NOISE_FILE))) {
                String name = scLine.next().substring(1);
                setColorFromName(name);
                int width = scLine.nextInt();
                int height = scLine.nextInt();
                data = new float[height][width];
            } else {
                int j = 0;
                while (scLine.hasNext()) {
                    float value = scLine.nextFloat();
                    if (data != null) {
                        data[i][j] = value;
                    } else {
                        throw new IllegalArgumentException("Invalid pattern file");
                    }
                }
                i+=1;
            }

        }
    }

    /**
     * This function returns a 2D array of floats representing the pixels of the channel.
     *
     * @return pixels of the channel.
     */
    public float[][] getData() {
        return data;
    }

    /**
     * This function returns the color of the channel.
     *
     * @return The color of the channel.
     */
    public Color getChannel() {
        return color;
    }

    /**
     * This function sets a 2D array of floats representing the pixels of the channel.
     *
     * @param data The pixels of the channel.
     */
    public void setData(float[][] data) {
        this.data = data;
    }

    /**
     * This function sets the color of the channel.
     *
     * @param color The color of the channel.
     */
    public void setChannel(Color color) {
        this.color = color;
    }

    /**
     * This function returns the value of a pixel at the given x and y coordinates.
     *
     * @param x The x coordinate of the pixel to get the value of.
     * @param y The y coordinate of the pixel to get the value of.
     * @return The value of a pixel at the given x and y coordinates.
     */
    public float getValue(int x, int y) {
        return data[x][y];
    }

    /**
     * This function sets the value of a pixel at the given x and y coordinates to the given value.
     *
     * @param x The x coordinate of the value to set.
     * @param y The y coordinate of the pixel to set.
     * @param value The value to set for a pixel at the given x and y coordinates.
     */
    public void setValue(int x, int y, float value) {
        data[x][y] = value;
    }

    /**
     * Returns the height of the color channel.
     *
     * @return The height of the color channel.
     */
    public int getHeight() {
        return data.length;
    }

    /**
     * Returns the width of the color channel.
     *
     * @return The width of the color channel.
     */
    public int getWidth() {
        return data[0].length;
    }

    /**
     * Set all the pixels of the channel to 0
     */
    public void clear() {
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[0].length; j++) {
                data[i][j] = 0;
            }
        }
    }

    /**
     * Return the name of the channel.
     *
     * @return The name of the color.
     */
    public String getColorName() {
        switch (color) {
        case RED:
            return Constant.RED_CHANNEL_NAME;
        case GREEN:
            return Constant.GREEN_CHANNEL_NAME;
        case BLUE:
            return Constant.BLUE_CHANNEL_NAME;
        default:
            throw new IllegalArgumentException("Unknown channel");
        }
    }

    public static Color getColorFromIndex(int index) {
        switch (index) {
        case 0:
            return RED;
        case 1:
            return GREEN;
        case 2:
            return BLUE;
        default:
            throw new IllegalArgumentException("Unknown channel");
        }
    }

    public static int getIndexFromColor(Color color) {
        switch (color) {
        case RED:
            return 0;
        case GREEN:
            return 1;
        case BLUE:
            return 2;
        default:
            throw new IllegalArgumentException("Unknown channel");
        }
    }

    /**
     * Set the color for the channel given a color name
     *
     * @param channelName The name of the channel.
     */
    public void setColorFromName(String channelName) {
        switch (channelName) {
            case Constant.RED_CHANNEL_NAME:
                color = Color.RED;
                break;
            case Constant.GREEN_CHANNEL_NAME:
                color = Color.GREEN;
                break;
            case Constant.BLUE_CHANNEL_NAME:
                color = Color.BLUE;
                break;
            default:
                throw new IllegalArgumentException("Unknown channel");
        }
    }

    /**
     * This function prints the object to the given PrintStream.
     *
     * @param ps The PrintStream object to print to.
     */
    protected void print(PrintStream ps) {
        ps.println(this);
    }

    /**
     * Prints the value of this object as an integer to the given PrintStream.
     *
     * @param ps The PrintStream object to print to.
     */
    protected void printAsInt(PrintStream ps) {
        ps.println(this.toString(true));
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ColorChannel) {
            ColorChannel other = (ColorChannel) obj;
            for (int i = 0; i < data.length; i++) {
                for (int j = 0; j < data[0].length; j++) {
                    if(data[i][j] != other.data[i][j]) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    /**
     * This function crops the image to the specified width and height and returns the values of the pixel as a 2D float array
     *
     * @param width the width of the cropped image
     * @param height the height of the cropped image
     * @return A 2D array of floats.
     * @throws IllegalArgumentException If the height and width of the image are greater than the height and width of the channel, then throw an exception.
     */
    public float[][] getCentralCropping(int width, int height) {
        if(height > data.length || width > data[0].length) {
            throw new IllegalArgumentException("Cropping size is larger than channel size");
        }
        int x = (data.length - height) / 2;
        int y = (data[0].length - width) / 2;
        int x_end = x + height;
        int y_end = y + width;
        float[][] cropped = new float[height][width];
        for(int i = x; i < x_end; i++) {
            if (y_end - y >= 0) System.arraycopy(data[i], y, cropped[i - x], y - y, y_end - y);
        }
        return cropped;
    }

    /**
     *  This function returns the channel as string in a csv-like style
     *
     * @param asInt if true, the values will be rounded to the nearest integer.
     * @return A string representation of the channel.
     */
    public String toString(boolean asInt) {
        StringBuilder sb = new StringBuilder();
        sb.append(Constant.LINE_START_FOR_CHANNEL_IN_NOISE_FILE).append(this.getColorName());
        sb.append(Constant.VALUE_SEPARATOR_FOR_NOISE_FILE).append(this.getWidth()).append(Constant.VALUE_SEPARATOR_FOR_NOISE_FILE).append(this.getHeight()).append("\n");
        for (float[] row : this.data) {
            for (float v : row) {
                if (asInt) {
                    sb.append((int) v);
                } else {
                    sb.append(v);
                }
                sb.append(Constant.VALUE_SEPARATOR_FOR_NOISE_FILE);
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
