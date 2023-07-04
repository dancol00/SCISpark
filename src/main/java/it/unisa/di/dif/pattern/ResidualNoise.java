package it.unisa.di.dif.pattern;

import it.unisa.di.dif.utils.Constant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * This class represent a Residual Noise of an image
 *
 * @author Andrea Bruno
* @author Paola Capasso
 */
public class ResidualNoise extends NoisePattern{
    /**
     * Default constructor
     */
    public ResidualNoise() {
        super();
    }

    /**
     * Generate a new Reference pattern with the specified height and width
     *
     * @param height the height of the new noise pattern
     * @param width  the width of the new noise pattern
     */
    public ResidualNoise(int height, int width) {
        super(height, width);
    }

    /**
     * Generate a new Reference pattern with the specified height and width and set all the channels to 0
     *
     * @param height the height of the image
     * @param width the width of the image
     * @return A new instance of ResidualNoise with the height and width of the image.
     */
    public static ResidualNoise getInstanceBySize(int height, int width) {
        ResidualNoise r = new ResidualNoise();
        float[][] red_data = new float[height][width];
        float[][] green_data = new float[height][width];
        float[][] blue_data = new float[height][width];
        r.setChannel(new ColorChannel(red_data, ColorChannel.Color.RED));
        r.setChannel(new ColorChannel(green_data, ColorChannel.Color.GREEN));
        r.setChannel(new ColorChannel(blue_data, ColorChannel.Color.BLUE));
        r.clear();
        return r;
    }

    /**
     * This function returns a new instance of the ResidualNoise class, which is the result of subtracting the two
     * GenericPattern objects passed as arguments
     *
     * @param a The first pattern
     * @param b the pattern to be subtracted from a
     * @return A new instance of ResidualNoise.
     */
    public static ResidualNoise getInstanceBySubtraction(GenericPattern a, GenericPattern b){
        return ResidualNoise.getInstantByOperation(a, b, "-");
    }

    /**
     * Get the instance of the ResidualNoise class by adding two GenericPatterns together.
     *
     * @param a The first pattern
     * @param b the pattern to be added to a
     * @return A new instance of ResidualNoise.
     */
    public static ResidualNoise getInstanceByAddition(GenericPattern a, GenericPattern b) {
        return ResidualNoise.getInstantByOperation(a, b, "+");
    }

    /**
     * This function returns a new ResidualNoise object that is the product of two GenericPattern objects.
     *
     * @param a The first pattern
     * @param b the pattern to be used as the base
     * @return A new instance of ResidualNoise.
     */
    public static ResidualNoise getInstanceByMultiplication(GenericPattern a, GenericPattern b) {
        return ResidualNoise.getInstantByOperation(a, b, "*");
    }

    /**
     * This function returns a new instance of the ResidualNoise class, which is the result of dividing the two
     * GenericPatterns passed as arguments
     *
     * @param a The first pattern
     * @param b the pattern to be divided by
     * @return A new instance of ResidualNoise.
     */
    public static ResidualNoise getInstanceByDivision(GenericPattern a, GenericPattern b) {
        return ResidualNoise.getInstantByOperation(a, b, "/");
    }

    private static ResidualNoise getInstantByOperation(GenericPattern a, GenericPattern b, String op){
        if(a.equalsSize(b)){
            ResidualNoise r = new ResidualNoise();
            for(ColorChannel.Color color : ColorChannel.Color.values()){
                r.setChannel(a.getColorChannel(color));
            }
            switch (op) {
                case "+":
                    r.add(b);
                    break;
                case "-":
                    r.subtract(b);
                    break;
                case "*":
                    r.multiply(b);
                    break;
                case "/":
                    r.divide(b);
                    break;
                default:
                    throw new IllegalArgumentException("Operation not supported");
            }
            return r;
        } else {
            throw new IllegalArgumentException("Patterns must be of the same size");
        }
    }

    /**
     * It takes a pattern and an integer, and returns a residual noise object with the pattern divided by the integer
     *
     * @param a The pattern to be divided
     * @param b The integer to divide by
     * @return A ResidualNoise object
     */
    public static ResidualNoise divideByInteger(GenericPattern a, int b) {
        if(b == 0){
            throw new IllegalArgumentException("Divisor cannot be zero");
        }
        ResidualNoise r = new ResidualNoise();
        float[][] red_data = new float[a.getHeight()][a.getWidth()];
        float[][] green_data = new float[a.getHeight()][a.getWidth()];
        float[][] blue_data = new float[a.getHeight()][a.getWidth()];

        for(int i = 0; i < a.getHeight(); i++){
            for(int j = 0; j < a.getWidth(); j++){
                red_data[i][j] = a.getValue(i, j, ColorChannel.Color.RED) * b;
                green_data[i][j] = a.getValue(i, j, ColorChannel.Color.GREEN) * b;
                blue_data[i][j] = a.getValue(i, j, ColorChannel.Color.BLUE) * b;
            }
        }

        r.setChannel(new ColorChannel(red_data, ColorChannel.Color.RED));
        r.setChannel(new ColorChannel(green_data, ColorChannel.Color.GREEN));
        r.setChannel(new ColorChannel(blue_data, ColorChannel.Color.BLUE));
        return r;
    }

    @Override
    public String toString() {
        return "ResidualNoise [width=" + this.getWidth() + ", height=" + this.getHeight() + "]";
    }

    @Override
    public ResidualNoise getCroppedPattern(int width, int height) {
        ResidualNoise copy = new ResidualNoise();
        for (ColorChannel.Color color : ColorChannel.Color.values()) {
            copy.setChannel(new ColorChannel(this.getRedChannel().getCentralCropping(width, height), color));
        }
        return copy;
    }

    public static ResidualNoise load(File f) throws IOException {
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
        int height = Integer.parseInt(split[split.length-1]);;
        ResidualNoise rp = new ResidualNoise(height, width);
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
