package it.unisa.di.dif.utils;

/**
 * Declare an enum type to define the action to perform with patterns of different size
 *
 * @author Andrea Bruno
 * @author Paola Capasso
 */
public enum AdaptationMethod {
    /**
     * Crop patterns with different sizes
     */
    CROP,
    /**
     * Resize patterns with different sizes
     * @deprecated
     */
    RESIZE,
    /**
     * Not perform any action
     */
    NOT_ADAPT
}
