package it.unisa.di.dif.pattern;

/**
 * Marker abstract class used to identify all the noise types (i.e. Reference Pattern and Residual Noise)
 *
 * @see ReferencePattern
 * @see ResidualNoise
 * @author Andrea Bruno
* @author Paola Capasso
 */
public abstract class NoisePattern extends GenericPattern {
    /**
     * Default constructor
     */
    public NoisePattern() {
        super();
    }


    /**
     * Generate a new Noise pattern with the specified height and width
     *
     * @param height the height of the new noise pattern
     * @param width  the width of the new noise pattern
     */
    public NoisePattern(int height, int width) {
        super(height, width);
    }

    @Override
    public abstract NoisePattern getCroppedPattern(int width, int height);

    @Override
    public String toString() {
        return "NoisePattern [width=" + this.getWidth() + ", height=" + this.getHeight() + "]";
    }
}
