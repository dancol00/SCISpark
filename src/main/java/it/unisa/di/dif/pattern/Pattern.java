package it.unisa.di.dif.pattern;

import java.io.File;
import java.nio.file.Path;

/**
 * It's an interface to define a generic pattern
 * @see Image
 * @see ResidualNoise
 * @see ReferencePattern
 * * @author Andrea Bruno
* @author Paola Capasso
 */
public interface Pattern {

    /**
     * Returns the color channel object for the specified channel
     *
     * @param color The channel to get.
     * @return A ColorChannel object.
     */
    public ColorChannel getColorChannel(ColorChannel.Color color);


    /**
     * Returns the red channel of the image
     *
     * @return A ColorChannel object.
     */
    public ColorChannel getRedChannel();

    /**
     * Returns the green channel of the image
     *
     * @return A ColorChannel object.
     */
    public ColorChannel getGreenChannel();

    /**
     * Returns the blue channel of the image
     *
     * @return A ColorChannel object.
     */
    public ColorChannel getBlueChannel();

    /**
     * Returns the height of the image
     *
     * @return The height of the image.
     */
    public int getHeight();

    /**
     * Returns the width of the image
     *
     * @return The width of the rectangle.
     */
    public int getWidth();

    /**
     * Returns the value of desired pixel in the specified channel
     *
     * @param x The x coordinate of the pixel you want to get the value of.
     * @param y The y coordinate of the pixel you want to get the value of.
     * @param color The channel to get the value from.
     * @return A float value of the pixel at the given x and y coordinates in the specified channel.
     */
    public float getValue(int x, int y, ColorChannel.Color color);

    /**
     * Returns the casted to integer value of desired pixel in the specified channel
     *
     * @param x The x coordinate of the pixel you want to get the value of.
     * @param y The y coordinate of the pixel you want to get the value of.
     * @param color The channel to get the value from.
     * @return A integer value of the pixel at the given x and y coordinates in the specified channel.
     */
    public int getValueAsInteger(int x, int y, ColorChannel.Color color);

    /**
     * Set all the pixels of the pattern to 0
     */
    public void clear();

    /**
     * Returns the name of the specified channel
     *
     * @param color The channel to get the name of.
     * @return The name of the channel.
     */
    public String getChannelName(ColorChannel.Color color);

    /**
     * Sets the height of the image.
     * 
     * @param height The height of the image in pixels.
     */
    public void setHeight(int height);
    
    /**
     * Sets the width of the image.
     * 
     * @param width The width of the image in pixels.
     */
    public void setWidth(int width);
    
    /**
     * Sets the channel data to be used for the red color
     * 
     * @param value The new value for the channel.
     */
    public void setChannel(ColorChannel value);
    
    /**
     * Sets the channel data for the red color
     * 
     * @param value The new value for the red channel.
     */
    public void setRedChannel(ColorChannel value);

    /**
     * Sets the channel data for the green color
     *
     * @param value The new value for the green channel.
     */
    public void setGreenChannel(ColorChannel value);

    /**
     * Sets the channel data for the blue color
     *
     * @param value The new value for the blue channel.
     */
    public void setBlueChannel(ColorChannel value);

    public void storeAsNpz(Path p, String name);
    public void storeAsNpz(String filename, String name);
    public void storeAsNpz(Path p);
    public void storeAsNpz(String filename);
    public float[][][] loadFromNpz(Path p);
    public float[][][] loadFromNpz(String filename);
    public float[][][] loadFromNpz(Path p, String name);
    public float[][][] loadFromNpz(String filename, String name);


    /**
     * This function stores the noise in a file in a csv-like style
     *
     * @param f The file to store the noise in.
     * @param asInt If true, the noise values will be stored as integer. If false, they will be stored as float.
     */
    public void storeNoise(File f, boolean asInt);

    /**
     * This function stores the noise as integer values in a file in a csv-like style
     *
     * @param f The file to store the noise in.
     */
    public void storeAsInteger(File f);

    /**
     * This function stores the noise as float values in a file in a csv-like style
     *
     * @param f The file to store the noise in.
     */
    public void storeAsFloat(File f);

    /**
     * Returns true if the size of this pattern is equal to the size of the other pattern
     *
     * @param other The other pattern to compare to.
     * @return true if the pattern have the same size, false otherwise.
     */
    public boolean equalsSize(Pattern other);

    /**
     * Returns true if the pattern is equal to the other pattern on the specified channel
     *
     * @param other The other pattern to compare to.
     * @param color The channel to compare.
     * @return true if the pattern is equal to the other pattern on the specified channel.
     */
    public boolean equalsChannel(Pattern other, ColorChannel.Color color);

    /**
     * This function crops the image to the specified width and height.
     *
     * A central cropping function will be used.
     *
     * @param width The width of the cropped image.
     * @param height The height of the cropped image
     */
    public void crop(int width, int height);

    /**
     * It takes a pattern and returns a new pattern that is a cropped version of the original pattern
     *
     *  A central cropping function will be used.
     *
     * @param width The width of the cropped pattern.
     * @param height The height of the pattern to be cropped.
     * @return A pattern object.
     */
    public Pattern getCroppedPattern(int width, int height);


}
