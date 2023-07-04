package it.unisa.di.dif;

import it.unisa.di.dif.correlation.Pearson;
import it.unisa.di.dif.filter.Filter;
import it.unisa.di.dif.filter.FilterFactory;
import it.unisa.di.dif.pattern.*;
import it.unisa.di.dif.utils.AdaptationMethod;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


/**
 * It's a class that contains static methods for extracting the reference pattern from a list of images, extracting the
 * residual noise from an image, and comparing the reference pattern with the residual noise or reference pattern
 *
 * @author Andrea Bruno
 * @author Paola Capasso
 */
public abstract class SCIManager {

    /**
     * Extracting the reference pattern from a list of images using a filter for noise extraction.
     * It takes a list of images, a filter, and an adaptation method, and returns a reference pattern
     *
     * @param images ArrayList of images to be used to create the reference pattern
     * @param filter the filter to be used for the extraction of the residual noise.
     * @param method The method used to adapt the images to the same size.
     * @return A reference pattern.
     */
    public static ReferencePattern extractReferencePattern(List<Path> images, Filter filter, AdaptationMethod method) throws IOException {
        if (method == AdaptationMethod.RESIZE) {
            throw new UnsupportedOperationException("Resize method is not supported yet");
        }

        if (method == AdaptationMethod.NOT_ADAPT) {
            Image old = null;
            for (Path p : images) {
                Image image = new Image(p.toFile());
                if (old == null || old.equalsSize(image)) {
                    old = image;
                } else {
                    throw new UnsupportedOperationException("Images must have the same size when selected NOT_ADAPT method");
                }
            }
        }

        if (method == AdaptationMethod.CROP) {
            Image old = null;
            int height = Integer.MAX_VALUE;
            int width = Integer.MAX_VALUE;
            for (Path p : images) {
                Image image = new Image(p.toFile());
                if (old == null) {
                    old = image;
                    continue;
                }
                if (!old.equalsSize(image)) {
                    height = Math.min(Math.min(old.getHeight(), image.getHeight()), height);
                    width = Math.min(Math.min(old.getWidth(), image.getWidth()), width);
                }
            }
            int finalWidth = width;
            int finalHeight = height;
            for (Path path : images) {
                Image image = new Image(path.toFile());
                image = image.getCroppedPattern(finalWidth, finalHeight);
                image.storeInFile(path.toFile());
            }
            System.gc();
//            images = images.stream()
//                    .map(image -> image.getCroppedPattern(finalWidth, finalHeight))
//                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }

        Image image0 = new Image(images.get(0).toFile());
        int height = image0.getHeight();
        int width = image0.getWidth();
        ReferencePattern rp = new ReferencePattern(height, width);
        int numImagesInRP = 0;
        for (Path p : images) {
            Image image = new Image(p.toFile());
            ResidualNoise rn = extractResidualNoise(image, filter);
            rp.add(rn);
            numImagesInRP++;
        }
        rp.divideByValue(numImagesInRP);

        return rp;
    }


    /**
     * Extracts a reference pattern from a list of images using a filter and an adaptation method.
     *
     * @param images An ArrayList of Image objects.
     * @param filter The filter to use.
     * @return A reference pattern.
     */
    public static ReferencePattern extractReferencePattern(List<Path> images, Filter filter) throws IOException {
        return extractReferencePattern(images, filter, AdaptationMethod.NOT_ADAPT);
    }

    /**
     * Extracts a reference pattern from a list of images using the default filter and the specified adaptation method
     *
     * @param images An ArrayList of Image objects.
     * @param method The method used to adapt the size of the images.
     * @return A reference pattern.
     */
    public static ReferencePattern extractReferencePattern(List<Path> images, AdaptationMethod method) throws IOException {
        return extractReferencePattern(images, FilterFactory.getDefaultFilter(), method);
    }

    /**
     * Extracting the residual noise from an image using a filter.
     *
     * @param image  the image from which to extract residual noise
     * @param filter The filter to use.
     * @return ResidualNoise
     */
    public static ResidualNoise extractResidualNoise(Image image, Filter filter) {
        Image filteredImage = new Image();
        for (ColorChannel.Color color : ColorChannel.Color.values()) {
            ColorChannel ch = image.getColorChannel(color);
            if (ch == null) {
                continue;
            }

            Filter f = FilterFactory.getDefaultFilter();
            if (filter == null) {
                throw new IllegalArgumentException("Error: filter is null");
            }

            ColorChannel chFiltered = f.getFiltered(ch);
            filteredImage.setChannel(chFiltered);
        }
        return ResidualNoise.getInstanceBySubtraction(image, filteredImage);
    }

    /**
     * This function extracts the reference pattern from a list of images using the default filter and the default adaptation method.
     *
     * @param images The images to extract the reference pattern from.
     * @return A reference pattern.
     */
    public static ReferencePattern extractReferencePattern(List<Path> images) throws IOException {
        return extractReferencePattern(images, FilterFactory.getDefaultFilter(), AdaptationMethod.NOT_ADAPT);
    }


    /**
     * It takes an image and a filter and returns a ResidualNoise object.
     *
     * @param image The image to extract the residual noise from.
     * @return A ResidualNoise object.
     */
    public static ResidualNoise extractResidualNoise(Image image) {
        return extractResidualNoise(image, FilterFactory.getDefaultFilter());
    }

    /**
     * It takes an image and returns a ResidualNoise object
     *
     * @param path The path to the image file.
     * @return A ResidualNoise object.
     */
    public static ResidualNoise extractResidualNoise(String path) throws IOException {
        return extractResidualNoise(new Image(path));
    }

    /**
     * It takes an image and a filter, and returns the residual noise of the image after applying the filter
     *
     * @param path   The path to the image file.
     * @param filter The filter to use.
     * @return ResidualNoise
     */
    public static ResidualNoise extractResidualNoise(String path, Filter filter) throws IOException {
        return extractResidualNoise(new Image(path), filter);
    }

    /**
     * It takes two noise patterns and a method of adaptation, and returns the Pearson correlation coefficient between the
     * two patterns
     *
     * @param referencePattern The reference pattern that you want to compare against.
     * @param residualNoise    The noise pattern that you want to compare to the reference pattern.
     * @param method           The method to use when the reference pattern and residual noise have different sizes.
     * @return The Pearson correlation coefficient.
     */
    public static double compare(ReferencePattern referencePattern, NoisePattern residualNoise, AdaptationMethod method) {
        if (!referencePattern.equalsSize(residualNoise)) {
            if (method == AdaptationMethod.NOT_ADAPT) {
                throw new IllegalArgumentException("Error: reference pattern and residual noise have different sizes and adaptation method is set to NOT_ADAPT");
            } else if (method == AdaptationMethod.RESIZE) {
                throw new UnsupportedOperationException("Resize method is not supported yet");
            } else if (method == AdaptationMethod.CROP) {
                int height = Math.min(referencePattern.getHeight(), residualNoise.getHeight());
                int width = Math.min(referencePattern.getWidth(), residualNoise.getWidth());
                referencePattern = referencePattern.getCroppedPattern(width, height);
                residualNoise = residualNoise.getCroppedPattern(width, height);
            }
        }
        Pearson p = new Pearson(referencePattern, residualNoise);

        return p.getCorrelationCoefficient();
    }

    /**
     * This function compares a reference pattern to a noise pattern and returns a similarity score
     *
     * @param referencePattern The reference pattern that you want to compare against.
     * @param residualNoise    The noise pattern to be compared to the reference pattern.
     * @return The similarity between the reference pattern and the residual noise.
     */
    public static double compare(ReferencePattern referencePattern, NoisePattern residualNoise) {
        return compare(referencePattern, residualNoise, AdaptationMethod.NOT_ADAPT);
    }
}
