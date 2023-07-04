package it.unisa.di.dif.correlation;

import it.unisa.di.dif.pattern.ColorChannel;
import it.unisa.di.dif.pattern.Pattern;

import java.util.ArrayList;
import java.util.OptionalDouble;

/**
 * Class implementing the Newman-Pearson correlation for Noise Patterns
 *
 * @author Andrea Bruno
* @author Paola Capasso
 */
public class Pearson {
    private double red_correlation = Double.NaN;
    private double green_correlation = Double.NaN;
    private double blue_correlation = Double.NaN;

    /**
     * Contructor that generates a Newman-Person correlation for Noise Pattern given its values.
     * @param red_correlation correlation value for red channel
     * @param green_correlation correlation value for green channel
     * @param blue_correlation correlation value for blue channel
     */
    public Pearson(double red_correlation, double green_correlation, double blue_correlation) {
        this.red_correlation = red_correlation;
        this.green_correlation = green_correlation;
        this.blue_correlation = blue_correlation;
    }


    /**
     * A constructor that takes two patterns as input and computes the Pearson correlation for each color channel.
     *
     * @param p1 First Noise Pattern.
     * @param p2 Second Noise Pattern.
     */
    public Pearson(Pattern p1, Pattern p2) {
        if(p1 == null || p2 == null) {
            throw new IllegalArgumentException("Patterns are null");
        }

        if(!p1.equalsSize(p2)) {
            throw new IllegalArgumentException("Patterns are not of the same size");
        }

        red_correlation = computeCorrelation(p1.getColorChannel(ColorChannel.Color.RED), p2.getColorChannel(ColorChannel.Color.RED));
        green_correlation = computeCorrelation(p1.getColorChannel(ColorChannel.Color.GREEN), p2.getColorChannel(ColorChannel.Color.GREEN));
        blue_correlation = computeCorrelation(p1.getColorChannel(ColorChannel.Color.BLUE), p2.getColorChannel(ColorChannel.Color.BLUE));
    }

    /**
     * Returns the value of the Newman-Pearson correlation for red channel, if calculated.
     *
     * @return The Newman-Pearson correlation for red channel.
     * @throws IllegalStateException If the value of Newman-Pearson correlation for red channel is NaN (is not calculated), throw an exception.
     */
    public double getRed_correlation() {
        if (Double.isNaN(red_correlation)) {
            throw new IllegalStateException("red_correlation is not set");
        }
        return red_correlation;
    }

    /**
     * This function sets the value of the correlation for the red channel
     *
     * @param red_correlation The value of the correlation for the red channel.
     */
    public void setRed_correlation(double red_correlation) {
        this.red_correlation = red_correlation;
    }

    /**
     * Returns the value of the Newman-Pearson correlation for green channel, if calculated.
     *
     * @return The Newman-Pearson correlation for green channel.
     * @throws IllegalStateException If the value of Newman-Pearson correlation for green channel is NaN (is not calculated), throw an exception.
     */
    public double getGreen_correlation() {
        if (Double.isNaN(green_correlation)) {
            throw new IllegalStateException("green_correlation is not set");
        }
        return green_correlation;
    }

    /**
     * This function sets the value of the correlation for the green channel
     *
     * @param green_correlation The value of the correlation for the green channel.
     */
    public void setGreen_correlation(double green_correlation) {
        this.green_correlation = green_correlation;
    }

    /**
     * Returns the value of the Newman-Pearson correlation for blue channel, if calculated.
     *
     * @return The Newman-Pearson correlation for blue channel.
     * @throws IllegalStateException If the value of Newman-Pearson correlation for blue channel is NaN (is not calculated), throw an exception.
     */
    public double getBlue_correlation() {
        if (Double.isNaN(blue_correlation)) {
            throw new IllegalStateException("blue_correlation is not set");
        }
        return blue_correlation;
    }

    /**
     * This function sets the value of the correlation for the blue channel
     *
     * @param blue_correlation The value of the correlation for the blue channel.
     */
    public void setBlue_correlation(double blue_correlation) {
        this.blue_correlation = blue_correlation;
    }

    private double computeCorrelation(ColorChannel p1, ColorChannel p2) {
        double mean_p1 = mean(p1);
        double mean_p2 = mean(p2);

        double sum = 0;
        double sum_p1 = 0;
        double sum_p2 = 0;

        for(int i = 0; i < p1.getHeight(); i++) {
            for(int j = 0; j < p1.getWidth(); j++) {
                sum += (p1.getValue(i, j) - mean_p1) * (p2.getValue(i, j) - mean_p2);
                sum_p1 += (p1.getValue(i, j) - mean_p1) * (p1.getValue(i, j) - mean_p1);
                sum_p2 += (p2.getValue(i, j) - mean_p2) * (p2.getValue(i, j) - mean_p2);
            }
        }

        double den = Math.sqrt(sum_p1) * Math.sqrt(sum_p2);
        return sum / den;
    }

    private double mean(ColorChannel p) {
        if (p == null||p.getHeight()==0) {
            throw new IllegalArgumentException("Pattern is null or empty");
        }

        double sum = 0;

        ArrayList<Double> averages = new ArrayList<>();

        for(int j=0; j<p.getWidth(); j++) {
            for(int i=0; i<p.getHeight(); i++) {
                sum += p.getValue(i,j);
            }
            averages.add(sum/p.getHeight());
            sum = 0;
        }
        OptionalDouble average =  averages.stream().mapToDouble(a -> a).average();

        return average.isPresent() ? average.getAsDouble() : Double.NaN;
    }

    @Override
    public String toString() {
        return "Pearson{" +
                "red_correlation=" + red_correlation +
                ", green_correlation=" + green_correlation +
                ", blue_correlation=" + blue_correlation +
                '}';
    }

    /**
     * It returns the average of the correlation coefficients of the red, green, and blue channels
     *
     * @return The average of the correlation coefficients of the red, green, and blue channels.
     */
    public double getCorrelationCoefficient() {
        return (getRed_correlation() + getGreen_correlation() + getBlue_correlation()) / 3;
    }
}
