package it.unisa.di.dif.pattern;

import it.unisa.di.dif.utils.CHILogger;
import it.unisa.di.dif.utils.Constant;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * It's a class that represents an image, and it's a subclass of the class GenericPattern
 *
 * @author Andrea Bruno
* @author Paola Capasso
 */
public class Image extends GenericPattern{
    private boolean isFiltered;

    /**
     * Default constructor
     */
    public Image() {
        super();
    }

    private void initImage(BufferedImage image) {
        int x,x1;

        int numeroRighe=image.getHeight();
        int numeroColonne=image.getWidth();

        setFiltered(false);

        float[][] canale_Red = new float[numeroRighe][numeroColonne];
        float[][] canale_Green = new float[numeroRighe][numeroColonne];
        float[][] canale_Blue = new float[numeroRighe][numeroColonne];

        for(int i=0;i<numeroRighe;i++){
            for(int j=0;j<numeroColonne;j++){
                x=image.getRGB(j, i); //il metodo prende prima le colonne e poi le righe
                x1=x&0x7FFFFFFF;
                canale_Red[i][j]=((x1/256)/256)%256;
                canale_Green[i][j]=(x1/256)%256;
                canale_Blue[i][j]=x1%256;
            }
        }

        this.setRedChannel(new ColorChannel(canale_Red, ColorChannel.Color.RED));
        this.setGreenChannel(new ColorChannel(canale_Green, ColorChannel.Color.GREEN));
        this.setBlueChannel(new ColorChannel(canale_Blue, ColorChannel.Color.BLUE));
    }

    public Image(BufferedImage image) {
        super();
        initImage(image);
    }

    /**
     * Reading the image from an input stream and storing the pixel values in the red, green and blue channels.
     *
     * @param is the input stream of the image
     * @throws IOException Error on reading file
     */
    public Image(InputStream is) throws IOException {
        super();

        BufferedImage image;

        try {
            image= ImageIO.read(is);
            initImage(image);

        } catch (IOException e) {

            Constant constant = Constant.getInstance();
            if(constant.isWriteMessageOnStderr())
                e.printStackTrace();

            CHILogger logger = CHILogger.getInstance();
            logger.log.fatal("Impossibile leggere lo stream di immagine ");

            throw e;
        }
    }

    /**
     * Reading the image file and storing the pixel values in the red, green and blue channels.
     *
     * @param f the original image file on disk
     * @throws IOException Error on reading file
     */
    public Image(File f) throws IOException {
        super();

        BufferedImage image;

        try {
            image= ImageIO.read(f);
            initImage(image);
        } catch (IOException e) {

            Constant constant = Constant.getInstance();
            if(constant.isWriteMessageOnStderr())
                e.printStackTrace();

            CHILogger logger = CHILogger.getInstance();
            logger.log.fatal("Impossibile leggere il file d'immagine "+f+".");

            throw e;
        }
    }

    /**
     * Reading the image file and storing the pixel values in the red, green and blue channels.
     *
     * @param path the original image file path on disk
     * @throws IOException Error on reading file
     */
    public Image(String path) throws IOException {
        this(new File(path));
    }

    /**
     * It takes the image and stores it in a file
     *
     * @param f the file to write to
     * @return true if the images is correctly stored, false otherwise.
     */
    public boolean storeInFile(File f){
        try{
            String format = "JPG";
            if(f.getName().toLowerCase().endsWith("png")){
                format = "PNG";
            }
            BufferedImage immagine = new BufferedImage(this.getWidth(), this.getHeight(), BufferedImage.TYPE_INT_RGB);

            int r = 0;
            int g = 0;
            int b = 0;
            int col = 0;

            for(int i=0; i<this.getHeight(); i++)
                for(int j=0; j<this.getWidth(); j++){

                    r = this.getValueAsInteger(i, j, ColorChannel.Color.RED); // red component 0...255
                    g = this.getValueAsInteger(i, j, ColorChannel.Color.GREEN); // green component 0...255
                    b = this.getValueAsInteger(i, j, ColorChannel.Color.BLUE); // blue component 0...255

                    col = (r << 16) | (g << 8) | b;

                    immagine.setRGB(j, i, col);
                }


            ImageIO.write(immagine, format, f); //Non voglio la compressione

        }

        catch(Exception e){
            return false; //ERRORE
        }

        return true; //Nessun errore (Scrittura riuscita)
    }

    /**
     * Indicates weather the image is filtered or not
     *
     * @return  true if the image is filtered, false otherwise
     */
    public boolean isFiltered() {
        return isFiltered;
    }

    /**
     * Setting the value of the boolean variable `isFiltered` to the value of the parameter `filtered`.
     *
     * @param filtered the value of isFiltered variable.
     */
    public void setFiltered(boolean filtered) {
        isFiltered = filtered;
    }

    @Override
    public String toString() {

        String msg="";

        if(this.isFiltered())
            msg="immagine filtrata";
        else
            msg="immagine originaria (non filtrata)";

        return "ImmagineFile [Base="+ this.getWidth()+", Altezza=" + this.getHeight() + ", "+msg + "]";
    }

    @Override
    public Image getCroppedPattern(int width, int height) {
        Image copy = new Image();
        copy.setRedChannel(new ColorChannel(this.getRedChannel().getCentralCropping(width, height), ColorChannel.RED));
        copy.setGreenChannel(new ColorChannel(this.getGreenChannel().getCentralCropping(width, height), ColorChannel.GREEN));
        copy.setBlueChannel(new ColorChannel(this.getBlueChannel().getCentralCropping(width, height), ColorChannel.BLUE));
        copy.setFiltered(this.isFiltered());
        return copy;
    }
}
