package it.unisa.di.dif.pattern;

import it.unisa.di.dif.utils.Constant;

import java.io.*;
import java.nio.file.Path;
import java.security.SecureRandom;

/**
 * This class represent a Residual Noise of an image
 *
 * @author Andrea Bruno
* @author Paola Capasso
 */
public class ReferencePattern extends NoisePattern{
    public ReferencePattern() {
        super();
    }

    /**
     * A constructor that takes a ResidualNoise object and creates a ReferencePattern object from it by copy.
     *
     * @param rn The residual noise to copy in the new reference pattern
     */
    public ReferencePattern(ResidualNoise rn) {
        super();
        this.setRedChannel(rn.getRedChannel());
        this.setGreenChannel(rn.getGreenChannel());
        this.setBlueChannel(rn.getBlueChannel());
    }

    /**
     * Generate a new Reference pattern with the specified height and width
     *
     * @param height the height of the new noise pattern
     * @param width  the width of the new noise pattern
     */
    public ReferencePattern(int height, int width) {
        super(height, width);
    }

    /**
     * It creates a new ReferencePattern object, fills it with random values, and returns it
     *
     * @param height The height of the reference pattern.
     * @param width The width of the reference pattern.
     * @return A random reference pattern.
     */
    public static ReferencePattern random(int height, int width) {
        SecureRandom random = new SecureRandom();

        ReferencePattern rp = new ReferencePattern();
        float[][] red = new float[height][width];
        float[][] green = new float[height][width];
        float[][] blue = new float[height][width];

        for(int i = 0; i < height; i++) {
            for(int j = 0; j < width; j++) {
                red[i][j] = random.nextFloat();
                green[i][j] = random.nextFloat();
                blue[i][j] = random.nextFloat();
            }
        }

        rp.setRedChannel(new ColorChannel(red, ColorChannel.Color.RED));
        rp.setGreenChannel(new ColorChannel(green, ColorChannel.Color.GREEN));
        rp.setBlueChannel(new ColorChannel(blue, ColorChannel.Color.BLUE));

        return rp;
    }

    @Override
    public String toString() {
        return "ReferencePattern [width=" + this.getWidth() + ", height=" + this.getHeight() + "]";
    }

    @Override
    public ReferencePattern getCroppedPattern(int width, int height) {
        ReferencePattern copy = new ReferencePattern();
        copy.setRedChannel(new ColorChannel(this.getRedChannel().getCentralCropping(width, height), ColorChannel.RED));
        copy.setGreenChannel(new ColorChannel(this.getGreenChannel().getCentralCropping(width, height), ColorChannel.GREEN));
        copy.setBlueChannel(new ColorChannel(this.getBlueChannel().getCentralCropping(width, height), ColorChannel.BLUE));
        return copy;
    }

    public static ReferencePattern load(File f) throws IOException {
        BufferedReader r = new BufferedReader(new FileReader(f));
        String line = r.readLine();
        if(line == null) {
            throw new IOException("File is empty");
        }
        String[] split = line.split(Constant.VALUE_SEPARATOR_FOR_NOISE_FILE);
        if(split.length < 2) {
            throw new IOException("Invalid file format");
        }
        int width = Integer.parseInt(split[split.length-2]);
        int height = Integer.parseInt(split[split.length-1]);
        ReferencePattern rp = new ReferencePattern(height, width);
        line = r.readLine();
        StringBuilder b = new StringBuilder().append(line).append("\n");
        while (line != null) {
            if(line.startsWith(Constant.LINE_START_FOR_CHANNEL_IN_NOISE_FILE+"")){
                b = new StringBuilder().append(line).append("\n");
            } else if(line.trim().equals("")) {
                ColorChannel c = new ColorChannel(b.toString());
                rp.setChannel(c);
            } else {
                b.append(line).append("\n");
            }

            line = r.readLine();
        }
        r.close();
        return rp;
    }
}
